package test

import (
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"github.com/hobro-11/util/geotrie"

	"github.com/stretchr/testify/assert"
)

func TestBinaryTrie(t *testing.T) {
	// *demo csv 생성*
	createDemoCsv()
	defer deleteDemoCsv()

	// *trie를 빌드 및 저장*
	csvFilePath := "geohash.csv"
	outPath := "geohash_trie.bin"

	if err := geotrie.BuildBinaryTrie(csvFilePath, outPath); err != nil {
		t.Errorf("Error building binary trie: %v", err)
	}

	// *trie를 로드*
	tr, err := geotrie.LoadBinaryTrie("geohash_trie.bin")
	if err != nil {
		t.Errorf("Error loading binary trie: %v", err)
	}
	defer tr.Close()
	defer os.Remove("geohash_trie.bin")

	// *geohash 검색*
	{
		// case 1 wydj50
		fid, err := geotrie.Search6(tr, "wydj50")
		if err != nil {
			t.Errorf("Error searching geohash: %v", err)
		}
		assert.Equal(t, 55, fid)

		// case 2 wydj55
		if fid, err = geotrie.Search6(tr, "wydj55"); err != nil {
			t.Errorf("Error searching geohash: %v", err)
		}
		assert.Equal(t, 3, fid)

		// case 3 wydm9q
		if fid, err = geotrie.Search6(tr, "wydm9q"); err != nil {
			t.Errorf("Error searching geohash: %v", err)
		}
		assert.Equal(t, 1, fid)

		// case 4 wydn00
		if fid, err = geotrie.Search6(tr, "wy0000"); err != nil {
			t.Errorf("Error searching geohash: %v", err)
		}
		assert.Equal(t, 0, fid)
	}
}

func createDemoCsv() {
	// 1. CSV 파일 생성
	file, err := os.Create("geohash.csv")
	if err != nil {
		fmt.Println("파일 생성 오류:", err)
		return
	}
	defer file.Close() // 함수 종료 시 파일 닫기

	// 2. CSV Writer 생성
	writer := csv.NewWriter(file)
	defer writer.Flush() // 버퍼에 있는 데이터를 파일에 쓰기 (중요!)

	// 3. 데이터 정의
	records := [][]string{
		{"fid", "geohash"},
		{"55", "wyd"},
		{"1", "wydm9q"},
		{"3", "wydj55"},
	}

	// 4. 데이터 쓰기 (WriteAll 사용)
	if err := writer.WriteAll(records); err != nil {
		fmt.Println("데이터 쓰기 오류:", err)
		return
	}

	fmt.Println("geohash.csv 파일이 성공적으로 생성되었습니다.")
}

func deleteDemoCsv() {
	os.Remove("geohash.csv")
}
