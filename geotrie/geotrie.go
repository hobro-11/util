package geotrie

import (
	"fmt"
	"util/geotrie/trie"
	"os"
	"path/filepath"
)

// binary trie를 생성하고 저장
// csv 파일은 fid, geohash로 구성, 순서도 일치해야함
func BuildBinaryTrie(csvFilePath, outPath string) error {
	currentDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("현재 디렉토리를 확인할 수 없습니다: %v", err)
	}

	t := trie.NewTrie()
	if err := t.LoadFromCSV(filepath.Join(currentDir, csvFilePath)); err != nil {
		return err
	}

	fmt.Println("바이너리 인덱스 파일을 생성하는 중...")
	b := trie.NewBinaryTrieIndex(filepath.Join(currentDir, outPath))
	if err := b.Save(t); err != nil {
		return err
	}

	return nil
}

// binary trie를 로드
func LoadBinaryTrie(binPath string) (*trie.BinaryTrieIndex, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("현재 디렉토리를 확인할 수 없습니다: %v", err)
	}

	b := trie.NewBinaryTrieIndex(filepath.Join(currentDir, binPath))
	if err := b.Load(); err != nil {
		return nil, err
	}
	return b, nil
}

// trie와 geohash로 fid 검색
// geohash는 6자리여야 함
func Search6(tr *trie.BinaryTrieIndex, geohash string) (int, error) {
	if len(geohash) != 6 {
		return 0, fmt.Errorf("geohash는 6자리여야 합니다: %s", geohash)
	}

	// 3자리 prefix 검색 먼저 실행
	if len(geohash) >= 3 {
		prefix := geohash[:3]
		fid, err := tr.SearchGeohash(prefix)
		if err != nil {
			return 0, err
		}

		// 3자리 검색에서 값이 없으면 바로 0 반환
		if fid == 0 {
			return 0, nil
		}

		// 3자리 검색에서 값이 있으면 전체 geohash로 검색
		fullFid, err := tr.SearchGeohash(geohash)
		if err != nil {
			return 0, err
		}

		// 전체 geohash 검색에서 값이 있으면 그 값 사용, 없으면 3자리 검색 결과 사용
		if fullFid > 0 {
			return fullFid, nil
		}
		return fid, nil
	}

	return 0, nil
}

