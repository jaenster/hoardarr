package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// JobRepo implements domain/download.JobRepository against SQLite.
//
// Save inserts the entire Job + Files + Segments tree in a single
// transaction (joins the ambient one if present); subsequent saves
// update job-level columns. Children are not bulk-rewritten on update;
// the orchestrator's UpdateSegmentBatch handles per-segment progress.
type JobRepo struct {
	db *DB
}

// Compile-time check.
var _ download.JobRepository = (*JobRepo)(nil)

// NewJobRepo wires the repo over db.
func NewJobRepo(db *DB) *JobRepo {
	return &JobRepo{db: db}
}

// Save persists the Job. If j.ID() == 0, it inserts the full tree and
// back-fills assigned ids via SetID; otherwise it updates the
// job-level columns only.
func (r *JobRepo) Save(ctx context.Context, j *download.Job) error {
	if j.ID() == 0 {
		return r.insert(ctx, j)
	}
	return r.update(ctx, j)
}

func (r *JobRepo) insert(ctx context.Context, j *download.Job) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO jobs(
			nzb_hash, name, category, priority, queue_order, source, state,
			total_bytes, done_bytes, failed_bytes,
			added_at, started_at, finished_at, error_msg, nzb_blob,
			fetch_recovery_vols
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		j.NZBHash(), j.Name(), j.Category(), j.Priority(), j.QueueOrder(), j.Source(), string(j.State()),
		j.TotalBytes(), j.DoneBytes(), j.FailedBytes(),
		j.AddedAt().UnixMilli(),
		nullableMillis(j.StartedAt()), nullableMillis(j.FinishedAt()),
		nullableString(j.ErrorMsg()),
		j.NZBBlob(),
		boolToInt(j.FetchRecoveryVols()),
	)
	if err != nil {
		if isNZBHashUniqueViolation(err) {
			return download.ErrDuplicateNZBHash
		}
		return fmt.Errorf("insert job: %w", err)
	}
	jobID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("job last_id: %w", err)
	}
	j.SetID(download.JobID(jobID))

	for _, f := range j.Files() {
		if err := r.insertFile(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

func (r *JobRepo) update(ctx context.Context, j *download.Job) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE jobs SET
			name = ?, category = ?, priority = ?, queue_order = ?, state = ?,
			total_bytes = ?, done_bytes = ?, failed_bytes = ?,
			started_at = ?, finished_at = ?, error_msg = ?,
			fetch_recovery_vols = ?
		WHERE id = ?
	`,
		j.Name(), j.Category(), j.Priority(), j.QueueOrder(), string(j.State()),
		j.TotalBytes(), j.DoneBytes(), j.FailedBytes(),
		nullableMillis(j.StartedAt()), nullableMillis(j.FinishedAt()),
		nullableString(j.ErrorMsg()),
		boolToInt(j.FetchRecoveryVols()),
		int64(j.ID()),
	)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	return nil
}

func (r *JobRepo) insertFile(ctx context.Context, f *download.File) error {
	groupsJSON, err := json.Marshal(f.Groups())
	if err != nil {
		return fmt.Errorf("marshal groups: %w", err)
	}
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO files(
			job_id, filename, poster, groups, size_bytes, state,
			segment_count, segments_done, is_par2, is_recovery_vol
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		int64(f.JobID()), f.Filename(), nullableString(f.Poster()), string(groupsJSON),
		f.SizeBytes(), string(f.State()),
		f.SegmentCount(), f.SegmentsDone(), boolToInt(f.IsPar2()), boolToInt(f.IsRecoveryVol()),
	)
	if err != nil {
		return fmt.Errorf("insert file %q: %w", f.Filename(), err)
	}
	fileID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("file last_id: %w", err)
	}
	f.SetID(download.FileID(fileID))

	for _, s := range f.Segments() {
		if err := r.insertSegment(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (r *JobRepo) insertSegment(ctx context.Context, s *download.Segment) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO segments(
			file_id, seq_index, message_id, bytes, state, attempts, last_error, file_offset
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		int64(s.FileID()), s.SeqIndex(), s.MessageID(), s.Bytes(),
		string(s.State()), s.Attempts(), nullableString(s.LastError()), s.FileOffset(),
	)
	if err != nil {
		return fmt.Errorf("insert segment: %w", err)
	}
	segID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("segment last_id: %w", err)
	}
	s.SetID(download.SegmentID(segID))
	return nil
}

// ByID loads the full job tree.
func (r *JobRepo) ByID(ctx context.Context, id download.JobID) (*download.Job, error) {
	row := r.db.QueryRowCtx(ctx, selectJobByID, int64(id))
	j, err := scanJob(row)
	if err != nil {
		return nil, err
	}
	if err := r.loadFiles(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

// ByNZBHash looks up a job by the dedupe key.
func (r *JobRepo) ByNZBHash(ctx context.Context, hash string) (*download.Job, error) {
	row := r.db.QueryRowCtx(ctx, selectJobByHash, hash)
	j, err := scanJob(row)
	if err != nil {
		return nil, err
	}
	if err := r.loadFiles(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

// List returns all jobs (most recent first by queue_order).
func (r *JobRepo) List(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobs(ctx, selectAllJobs)
}

// ListShallow returns all jobs with file metadata populated but
// WITHOUT individual segments. Use for queue listings: the per-file
// summary columns (segment_count, segments_done) cover everything the
// UI needs, and skipping the N-extra-queries-per-file segment load
// is a 50× speedup on large releases.
//
// Callers that actually need to enumerate segments (orchestrator
// runners) must use ByID — that path still hydrates fully.
func (r *JobRepo) ListShallow(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobsShallow(ctx, selectAllJobs)
}

// ListJobsOnly returns all jobs with NO files at all. Use this when
// callers only need job-level summary fields (state, totals, names) —
// the queue list endpoint hits this hundreds of times per minute under
// active downloads and Sonarr polling, and the per-job files query is
// the dominant cost.
func (r *JobRepo) ListJobsOnly(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobsBare(ctx, selectAllJobs)
}

// Active returns jobs in non-terminal states ordered by priority.
func (r *JobRepo) Active(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobs(ctx, selectActiveJobs)
}

// ActiveShallow is Active without per-segment hydration. See
// ListShallow for the motivation.
func (r *JobRepo) ActiveShallow(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobsShallow(ctx, selectActiveJobs)
}

// ActiveJobsOnly returns active jobs with NO files attached. See
// ListJobsOnly for the motivation. This is the hot path for the
// /api/v1/queue and SAB queue endpoints.
func (r *JobRepo) ActiveJobsOnly(ctx context.Context) ([]*download.Job, error) {
	return r.queryJobsBare(ctx, selectActiveJobs)
}

// CountAll returns the total number of jobs. Used by /api/v1/system/status
// which only displays queue depth — no need to materialize aggregates.
func (r *JobRepo) CountAll(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CountActive returns the number of jobs in non-terminal states.
// Used by /api/v1/system/status.
func (r *JobRepo) CountActive(ctx context.Context) (int, error) {
	var n int
	const q = `SELECT COUNT(*) FROM jobs WHERE state IN ('queued','downloading','paused','download_complete','verifying','repairing','unpacking','waiting_for_server')`
	if err := r.db.QueryRowCtx(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// History returns terminal-state jobs (completed/failed/aborted) ordered
// by finished_at DESC, with optional Since / Category / State filters.
//
// Limit is clamped to [1, 500]. Zero or negative → 100. The clamp is
// hard so a misbehaving client can't drag the whole archive into memory.
//
// State filter: if the caller asks for a non-terminal state we still
// constrain to terminal rows — we never return active jobs from history.
func (r *JobRepo) History(ctx context.Context, q download.HistoryQuery) ([]*download.Job, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	// Build the WHERE clause incrementally. Args are positional in the
	// order they appear in the clause; we keep both lists in lockstep.
	clauses := []string{"state IN ('completed','failed','aborted')"}
	args := []any{}
	if q.State != "" && (q.State == download.JobStateCompleted ||
		q.State == download.JobStateFailed ||
		q.State == download.JobStateAborted) {
		clauses = []string{"state = ?"}
		args = append(args, string(q.State))
	}
	if q.Since != nil {
		clauses = append(clauses, "finished_at > ?")
		args = append(args, q.Since.UnixMilli())
	}
	if q.Category != "" {
		clauses = append(clauses, "category = ?")
		args = append(args, q.Category)
	}
	where := "WHERE " + strings.Join(clauses, " AND ")
	query := `SELECT ` + jobColumns + ` FROM jobs ` + where +
		` ORDER BY finished_at DESC, id DESC LIMIT ?`
	args = append(args, limit)

	return r.queryJobs(ctx, query, args...)
}

// HistoryJobsOnly is History with NO files attached — even cheaper
// than HistoryShallow. Use for the SAB history endpoint and the REST
// /api/v1/history list (Sonarr polls them aggressively). Per-file
// breakdown is only needed on the job-detail page, which uses ByID.
func (r *JobRepo) HistoryJobsOnly(ctx context.Context, q download.HistoryQuery) ([]*download.Job, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	clauses := []string{"state IN ('completed','failed','aborted')"}
	args := []any{}
	if q.State != "" && (q.State == download.JobStateCompleted ||
		q.State == download.JobStateFailed ||
		q.State == download.JobStateAborted) {
		clauses = []string{"state = ?"}
		args = append(args, string(q.State))
	}
	if q.Since != nil {
		clauses = append(clauses, "finished_at > ?")
		args = append(args, q.Since.UnixMilli())
	}
	if q.Category != "" {
		clauses = append(clauses, "category = ?")
		args = append(args, q.Category)
	}
	where := "WHERE " + strings.Join(clauses, " AND ")
	query := `SELECT ` + jobColumns + ` FROM jobs ` + where +
		` ORDER BY finished_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	return r.queryJobsBare(ctx, query, args...)
}

// HistoryShallow is History without per-file segment hydration. Used
// by the SAB history endpoint (which Sonarr polls every ~minute) and
// the REST /api/v1/history list — neither needs individual segments.
// Same query, same filters, same limits as History; only the
// hydration depth differs.
func (r *JobRepo) HistoryShallow(ctx context.Context, q download.HistoryQuery) ([]*download.Job, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	clauses := []string{"state IN ('completed','failed','aborted')"}
	args := []any{}
	if q.State != "" && (q.State == download.JobStateCompleted ||
		q.State == download.JobStateFailed ||
		q.State == download.JobStateAborted) {
		clauses = []string{"state = ?"}
		args = append(args, string(q.State))
	}
	if q.Since != nil {
		clauses = append(clauses, "finished_at > ?")
		args = append(args, q.Since.UnixMilli())
	}
	if q.Category != "" {
		clauses = append(clauses, "category = ?")
		args = append(args, q.Category)
	}
	where := "WHERE " + strings.Join(clauses, " AND ")
	query := `SELECT ` + jobColumns + ` FROM jobs ` + where +
		` ORDER BY finished_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	return r.queryJobsShallow(ctx, query, args...)
}

// Delete removes the job (cascades to files/segments).
func (r *JobRepo) Delete(ctx context.Context, id download.JobID) error {
	res, err := r.db.ExecCtx(ctx, `DELETE FROM jobs WHERE id = ?`, int64(id))
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return download.ErrJobNotFound
	}
	return nil
}

// UpdateSegmentBatch applies many segment updates in a single tx.
// Used by the orchestrator's 100ms drainer.
func (r *JobRepo) UpdateSegmentBatch(ctx context.Context, updates []download.SegmentUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	const stmt = `UPDATE segments SET state = ?, attempts = ?, last_error = ?, file_offset = ? WHERE id = ?`
	for _, u := range updates {
		if _, err := r.db.ExecCtx(ctx, stmt,
			string(u.State), u.Attempts, nullableString(u.LastError),
			u.FileOffset, int64(u.SegmentID),
		); err != nil {
			return fmt.Errorf("update segment %d: %w", u.SegmentID, err)
		}
	}
	return nil
}

func (r *JobRepo) queryJobs(ctx context.Context, query string, args ...any) ([]*download.Job, error) {
	rows, err := r.db.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*download.Job
	for rows.Next() {
		j, err := scanJobRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, j := range out {
		if err := r.loadFiles(ctx, j); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// queryJobsBare returns jobs with neither files nor segments loaded.
// Use when callers only need job-level summary fields (state, totals,
// names) — saves the N file queries per list call. The hot path for
// the queue and history endpoints under polling pressure (Sonarr).
func (r *JobRepo) queryJobsBare(ctx context.Context, query string, args ...any) ([]*download.Job, error) {
	rows, err := r.db.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*download.Job
	for rows.Next() {
		j, err := scanJobRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// queryJobsShallow is queryJobs without per-file segment hydration.
// File metadata (filename, size, segment counts, state) is populated
// from the files table; segments stay nil. Suitable for UI lists.
func (r *JobRepo) queryJobsShallow(ctx context.Context, query string, args ...any) ([]*download.Job, error) {
	rows, err := r.db.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*download.Job
	for rows.Next() {
		j, err := scanJobRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, j := range out {
		if err := r.loadFilesShallow(ctx, j); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// loadFilesShallow loads file metadata only; segments stay nil. Used
// by the queue list endpoint to avoid N+M segment queries per refresh.
func (r *JobRepo) loadFilesShallow(ctx context.Context, j *download.Job) error {
	rows, err := r.db.QueryCtx(ctx, selectFilesForJob, int64(j.ID()))
	if err != nil {
		return fmt.Errorf("query files: %w", err)
	}
	defer rows.Close()
	var files []*download.File
	for rows.Next() {
		var (
			fid           int64
			jobID         int64
			filename      string
			poster        sql.NullString
			groupsJSON    string
			sizeBytes     int64
			state         string
			segmentCount  int
			segmentsDone  int
			isPar2        int
			isRecoveryVol int
		)
		if err := rows.Scan(&fid, &jobID, &filename, &poster, &groupsJSON, &sizeBytes, &state, &segmentCount, &segmentsDone, &isPar2, &isRecoveryVol); err != nil {
			return fmt.Errorf("scan file: %w", err)
		}
		var groups []string
		if groupsJSON != "" {
			_ = json.Unmarshal([]byte(groupsJSON), &groups)
		}
		files = append(files, download.HydrateFile(download.HydrateFileParams{
			ID:            download.FileID(fid),
			JobID:         download.JobID(jobID),
			Filename:      filename,
			Poster:        poster.String,
			Groups:        groups,
			SizeBytes:     sizeBytes,
			State:         download.FileState(state),
			SegmentCount:  segmentCount,
			SegmentsDone:  segmentsDone,
			IsPar2:        isPar2 != 0,
			IsRecoveryVol: isRecoveryVol != 0,
			// Segments intentionally nil — shallow load.
		}))
	}
	*j = *rehydrateJob(j, files)
	return nil
}

func (r *JobRepo) loadFiles(ctx context.Context, j *download.Job) error {
	rows, err := r.db.QueryCtx(ctx, selectFilesForJob, int64(j.ID()))
	if err != nil {
		return fmt.Errorf("query files: %w", err)
	}
	type fileRow struct {
		f     *download.File
		fid   int64
	}
	var fileRows []fileRow
	for rows.Next() {
		var (
			fid           int64
			jobID         int64
			filename      string
			poster        sql.NullString
			groupsJSON    string
			sizeBytes     int64
			state         string
			segmentCount  int
			segmentsDone  int
			isPar2        int
			isRecoveryVol int
		)
		if err := rows.Scan(&fid, &jobID, &filename, &poster, &groupsJSON, &sizeBytes, &state, &segmentCount, &segmentsDone, &isPar2, &isRecoveryVol); err != nil {
			rows.Close()
			return fmt.Errorf("scan file: %w", err)
		}
		var groups []string
		if groupsJSON != "" {
			_ = json.Unmarshal([]byte(groupsJSON), &groups)
		}
		f := download.HydrateFile(download.HydrateFileParams{
			ID:            download.FileID(fid),
			JobID:         download.JobID(jobID),
			Filename:      filename,
			Poster:        poster.String,
			Groups:        groups,
			SizeBytes:     sizeBytes,
			State:         download.FileState(state),
			SegmentCount:  segmentCount,
			SegmentsDone:  segmentsDone,
			IsPar2:        isPar2 != 0,
			IsRecoveryVol: isRecoveryVol != 0,
			// Segments populated below.
		})
		fileRows = append(fileRows, fileRow{f: f, fid: fid})
	}
	rows.Close()

	// Load segments per file.
	for i := range fileRows {
		segs, err := r.loadSegments(ctx, download.FileID(fileRows[i].fid))
		if err != nil {
			return fmt.Errorf("load segments for file %d: %w", fileRows[i].fid, err)
		}
		// Re-hydrate the file with its segments populated.
		fileRows[i].f = rehydrateWithSegments(fileRows[i].f, segs)
	}

	// Re-hydrate the job with its files populated.
	files := make([]*download.File, len(fileRows))
	for i, fr := range fileRows {
		files[i] = fr.f
	}
	*j = *rehydrateJob(j, files)
	return nil
}

// rehydrateWithSegments rebuilds a File with its segments populated.
// We can't just append to the slice because File's segments field is
// private; the domain exposes Segments() as a copy. Use HydrateFile.
func rehydrateWithSegments(f *download.File, segs []*download.Segment) *download.File {
	return download.HydrateFile(download.HydrateFileParams{
		ID:            f.ID(),
		JobID:         f.JobID(),
		Filename:      f.Filename(),
		Poster:        f.Poster(),
		Groups:        f.Groups(),
		SizeBytes:     f.SizeBytes(),
		State:         f.State(),
		SegmentCount:  f.SegmentCount(),
		SegmentsDone:  f.SegmentsDone(),
		IsPar2:        f.IsPar2(),
		IsRecoveryVol: f.IsRecoveryVol(),
		Segments:      segs,
	})
}

// rehydrateJob is the symmetric helper for Jobs once their files are
// loaded.
func rehydrateJob(j *download.Job, files []*download.File) *download.Job {
	return download.HydrateJob(download.HydrateJobParams{
		ID:                j.ID(),
		NZBHash:           j.NZBHash(),
		Name:              j.Name(),
		Category:          j.Category(),
		Priority:          j.Priority(),
		QueueOrder:        j.QueueOrder(),
		Source:            j.Source(),
		State:             j.State(),
		TotalBytes:        j.TotalBytes(),
		DoneBytes:         j.DoneBytes(),
		FailedBytes:       j.FailedBytes(),
		AddedAt:           j.AddedAt(),
		StartedAt:         j.StartedAt(),
		FinishedAt:        j.FinishedAt(),
		ErrorMsg:          j.ErrorMsg(),
		NZBBlob:           j.NZBBlob(),
		Files:             files,
		FetchRecoveryVols: j.FetchRecoveryVols(),
	})
}

func (r *JobRepo) loadSegments(ctx context.Context, fileID download.FileID) ([]*download.Segment, error) {
	rows, err := r.db.QueryCtx(ctx, selectSegmentsForFile, int64(fileID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*download.Segment
	for rows.Next() {
		var (
			sid        int64
			fid        int64
			seqIndex   int
			msgID      string
			bytesN     int64
			state      string
			attempts   int
			lastError  sql.NullString
			fileOffset int64
		)
		if err := rows.Scan(&sid, &fid, &seqIndex, &msgID, &bytesN, &state, &attempts, &lastError, &fileOffset); err != nil {
			return nil, err
		}
		out = append(out, download.HydrateSegment(download.HydrateSegmentParams{
			ID:         download.SegmentID(sid),
			FileID:     download.FileID(fid),
			SeqIndex:   seqIndex,
			MessageID:  msgID,
			Bytes:      bytesN,
			State:      download.SegmentState(state),
			Attempts:   attempts,
			LastError:  lastError.String,
			FileOffset: fileOffset,
		}))
	}
	return out, rows.Err()
}

const jobColumns = `id, nzb_hash, name, category, priority, queue_order, source, state,
		total_bytes, done_bytes, failed_bytes,
		added_at, started_at, finished_at, error_msg, nzb_blob,
		fetch_recovery_vols`

const selectJobByID = `SELECT ` + jobColumns + ` FROM jobs WHERE id = ?`
const selectJobByHash = `SELECT ` + jobColumns + ` FROM jobs WHERE nzb_hash = ?`
const selectAllJobs = `SELECT ` + jobColumns + ` FROM jobs ORDER BY priority ASC, queue_order ASC`
const selectActiveJobs = `SELECT ` + jobColumns + ` FROM jobs
	WHERE state IN ('queued','downloading','paused','download_complete','verifying','repairing','unpacking')
	ORDER BY priority ASC, queue_order ASC`

const selectFilesForJob = `SELECT id, job_id, filename, poster, groups, size_bytes, state,
	segment_count, segments_done, is_par2, is_recovery_vol
	FROM files WHERE job_id = ? ORDER BY id ASC`

const selectSegmentsForFile = `SELECT id, file_id, seq_index, message_id, bytes, state,
	attempts, last_error, file_offset
	FROM segments WHERE file_id = ? ORDER BY seq_index ASC`

func scanJob(row *sql.Row) (*download.Job, error) {
	j, err := scanJobFromScanner(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, download.ErrJobNotFound
	}
	return j, err
}

func scanJobRows(s serverScanner) (*download.Job, error) {
	return scanJobFromScanner(s)
}

func scanJobFromScanner(s serverScanner) (*download.Job, error) {
	var (
		id              int64
		nzbHash         string
		name            string
		category        string
		priority        int
		queueOrder      int64
		source          string
		state           string
		totalBytes      int64
		doneBytes       int64
		failedBytes     int64
		addedAt         int64
		startedAt       sql.NullInt64
		finishedAt      sql.NullInt64
		errorMsg        sql.NullString
		nzbBlob         []byte
		fetchRecoveryV  int
	)
	if err := s.Scan(&id, &nzbHash, &name, &category, &priority, &queueOrder, &source, &state,
		&totalBytes, &doneBytes, &failedBytes,
		&addedAt, &startedAt, &finishedAt, &errorMsg, &nzbBlob,
		&fetchRecoveryV); err != nil {
		return nil, err
	}
	return download.HydrateJob(download.HydrateJobParams{
		ID:                download.JobID(id),
		NZBHash:           nzbHash,
		Name:              name,
		Category:          category,
		Priority:          priority,
		QueueOrder:        queueOrder,
		Source:            source,
		State:             download.JobState(state),
		TotalBytes:        totalBytes,
		DoneBytes:         doneBytes,
		FailedBytes:       failedBytes,
		AddedAt:           time.UnixMilli(addedAt).UTC(),
		StartedAt:         nullableTime(startedAt),
		FinishedAt:        nullableTime(finishedAt),
		ErrorMsg:          errorMsg.String,
		NZBBlob:           append([]byte(nil), nzbBlob...),
		FetchRecoveryVols: fetchRecoveryV != 0,
	}), nil
}

func nullableTime(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMilli(v.Int64).UTC()
}

func nullableMillis(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UnixMilli()
}

// isNZBHashUniqueViolation detects the modernc/sqlite error string for
// a UNIQUE constraint failure on jobs.nzb_hash.
//
// modernc.org/sqlite formats these as "constraint failed: UNIQUE
// constraint failed: jobs.nzb_hash (2067)". Matching by substring is
// brittle but pragmatic — the alternative is asserting on the
// concrete error type, which the driver reserves for future use and
// could change. Worst case of a miss: the error bubbles up wrapped
// instead of as our sentinel; AddJob falls through to the generic
// failure path.
func isNZBHashUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed: jobs.nzb_hash") ||
		strings.Contains(msg, "UNIQUE constraint failed: jobs_nzb_hash")
}
