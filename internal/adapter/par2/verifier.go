package par2

import (
	"bufio"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jaenster/hoardarr/internal/domain/verify"
)

// quickCheckIgnoreExts mirrors SABnzbd's quick-check-ignore list
// (newsunpack.py around line 1588). When a PAR2 FileDesc names one of
// these sidecar formats and the file isn't on disk, we skip it
// silently — they're optional metadata that NZB posters often drop,
// and a missing .nfo or .sfv should not block a release with an
// intact main file.
//
// A file that IS on disk but mismatches MD5 still fails normally;
// this only suppresses the "missing entirely" case.
var quickCheckIgnoreExts = map[string]struct{}{
	".nfo":    {},
	".sfv":    {},
	".srr":    {},
	".srt":    {},
	".idx":    {},
	".sub":    {},
	".jpg":    {},
	".jpeg":   {},
	".png":    {},
	".txt":    {},
	".readme": {},
}

func isQuickCheckIgnorable(name string) bool {
	_, ok := quickCheckIgnoreExts[strings.ToLower(filepath.Ext(name))]
	return ok
}

// Verifier implements verify.Verifier using PAR2 file metadata.
//
// Verification policy: full-file MD5 only. Per-slice MD5+CRC32 from
// IFSC packets is parsed and stored in the parsed structures for
// M3b's repair worker, but the verify pass keeps things simple by
// hashing each file once. This catches all corruption equally;
// per-slice precision is only useful for *locating* damage.
type Verifier struct{}

// Compile-time check.
var _ verify.Verifier = Verifier{}

// Verify reads PAR2 metadata from par2Paths and hashes each file in
// dataPaths against its declared MD5.
//
// Result.Files contains one FileResult per file declared in the
// PAR2 set. A file declared in PAR2 but absent from dataPaths
// (caller's mapping) is reported as not-OK with "not in NZB".
func (Verifier) Verify(_ context.Context, par2Paths []string, dataPaths map[string]string) (verify.Result, error) {
	set, err := parseFiles(par2Paths)
	if err != nil {
		return verify.Result{}, fmt.Errorf("parse par2: %w", err)
	}

	// Build a content-addressed index of the data files. Obfuscated
	// releases routinely ship with NZB-level filenames different from
	// the PAR2-recorded ones, and PAR2's FileDesc.MD516k exists
	// specifically so verify/repair can survive that. SAB does the same.
	md5Index := buildMD516kIndex(dataPaths)

	out := verify.Result{}
	for _, pf := range set.Files {
		fr := verify.FileResult{Filename: pf.Name}
		path, ok := dataPaths[pf.Name]
		if !ok {
			if p, ok2 := md5Index[pf.MD516k]; ok2 {
				path = p
				ok = true
			}
		}
		if !ok {
			// SAB-style quick-check ignore: optional sidecars (.nfo,
			// .sfv, subtitles, art) often go missing on release-posting
			// drops and shouldn't fail verify for the rest of the set.
			// Drop the FileResult entirely so it doesn't even count
			// against the "any not-OK = RepairNeeded" downstream rule.
			if isQuickCheckIgnorable(pf.Name) {
				continue
			}
			fr.Reason = "not in NZB"
			out.Files = append(out.Files, fr)
			continue
		}
		matched, reason := verifyFile(path, pf.MD5, pf.Size)
		fr.OK = matched
		fr.Reason = reason
		out.Files = append(out.Files, fr)
	}
	return out, nil
}

// parseFiles reads each PAR2 file in turn into a single consolidated
// RecoverySet (deduping repeated descriptive packets across .par2 +
// .vol* files).
func parseFiles(paths []string) (*RecoverySet, error) {
	if len(paths) == 0 {
		return nil, ErrNoPar2
	}
	set := &RecoverySet{}
	seen := false
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", p, err)
		}
		err = parsePackets(bufio.NewReader(f), func(pkt Packet) error {
			if !seen {
				set.SetID = pkt.SetID
				seen = true
			} else if set.SetID != pkt.SetID {
				return fmt.Errorf("par2: file %q has set_id %x; expected %x", p, pkt.SetID, set.SetID)
			}
			return set.consume(pkt)
		})
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
	}
	return set, nil
}

// verifyFile MD5s the file at path and compares to expectMD5. Returns
// (matched, reason). Size mismatch short-circuits.
func verifyFile(path string, expectMD5 [16]byte, expectSize uint64) (bool, string) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, "missing on disk"
		}
		return false, fmt.Sprintf("open: %v", err)
	}
	defer f.Close()

	if expectSize > 0 {
		st, err := f.Stat()
		if err != nil {
			return false, fmt.Sprintf("stat: %v", err)
		}
		if uint64(st.Size()) != expectSize {
			return false, fmt.Sprintf("size %d; want %d", st.Size(), expectSize)
		}
	}

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, fmt.Sprintf("read: %v", err)
	}
	var sum [16]byte
	copy(sum[:], h.Sum(nil))
	if sum != expectMD5 {
		return false, "md5 mismatch"
	}
	return true, ""
}
