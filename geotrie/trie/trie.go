package trie

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// TrieNode는 Geohash Trie의 노드를 나타냅니다.
type TrieNode struct {
	Children map[byte]*TrieNode
	IsEnd    bool
	FID      int
}

// NewTrieNode는 새로운 TrieNode를 생성합니다.
func NewTrieNode() *TrieNode {
	return &TrieNode{
		Children: make(map[byte]*TrieNode),
		IsEnd:    false,
		FID:      0,
	}
}

// Trie는 Geohash를 저장하는 Trie 자료구조입니다.
type Trie struct {
	Root *TrieNode
}

// NewTrie는 새로운 Trie를 생성합니다.
func NewTrie() *Trie {
	return &Trie{
		Root: NewTrieNode(),
	}
}

// Insert는 Trie에 geohash와 fid를 삽입합니다.
func (t *Trie) Insert(geohash string, fid int) {
	node := t.Root
	for i := 0; i < len(geohash); i++ {
		char := geohash[i]
		if _, exists := node.Children[char]; !exists {
			node.Children[char] = NewTrieNode()
		}
		node = node.Children[char]
	}
	node.IsEnd = true
	node.FID = fid
}

// Search는 주어진 geohash에 대한 FID를 찾습니다.
// 정확한 매치가 없으면 가장 긴 prefix에 해당하는 FID를 반환합니다.
func (t *Trie) Search(geohash string) int {
	node := t.Root
	tmp := 0

	for i := 0; i < len(geohash); i++ {
		char := geohash[i]
		
		// 현재 노드가 끝 노드라면 FID 업데이트
		if node.IsEnd {
			tmp = node.FID
		}
		
		// 다음 문자에 해당하는 자식 노드가 없으면 현재까지의 FID 반환
		if _, exists := node.Children[char]; !exists {
			return tmp
		}
		
		node = node.Children[char]
	}
	
	// 마지막 노드가 끝 노드라면 해당 FID 반환, 아니면 마지막으로 업데이트된 FID 반환
	if node.IsEnd {
		return node.FID
	}
	
	return tmp
}

// LoadFromCSV는 CSV 파일에서 geohash와 fid를 읽어 Trie에 로드합니다.
func (t *Trie) LoadFromCSV(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("CSV 파일을 열 수 없습니다: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	
	// 헤더 건너뛰기
	if scanner.Scan() {
		// 첫 줄은 헤더이므로 무시
	}

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			continue
		}

		// FID 추출
		fidStr := strings.TrimSpace(parts[0])
		fid := 0
		fmt.Sscanf(fidStr, "%d", &fid)
		
		// geohash 추출 및 공백 제거
		geohash := strings.TrimSpace(parts[1])
		
		// 비어있지 않은 geohash만 처리
		if geohash != "" {
			// log.Printf("Loading: FID=%d, Geohash=%s", fid, geohash)
			t.Insert(geohash, fid)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("CSV 파일 읽기 오류: %v", err)
	}

	return nil
}