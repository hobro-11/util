package trie

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"syscall"
)

const (
	// 노드 구조체 크기 (바이트 단위)
	// 1바이트(문자) + 1바이트(IsEnd) + 4바이트(FID) + 4바이트(자식 수) + 자식 배열 오프셋(8바이트)
	NODE_HEADER_SIZE = 10
	// 각 자식 포인터는 1바이트(문자) + 8바이트(노드 오프셋)
	CHILD_POINTER_SIZE = 9
	// 파일 헤더 크기: 매직 넘버(4바이트) + 버전(4바이트) + 루트 노드 오프셋(8바이트) + 노드 수(4바이트)
	FILE_HEADER_SIZE = 20
	// 매직 넘버 (GTRI - Geohash TRIE)
	MAGIC_NUMBER = 0x47545249
	// 파일 버전
	FILE_VERSION = 1
)

// BinaryTrieNode는 바이너리 파일에 저장될 Trie 노드의 메모리 표현입니다.
type BinaryTrieNode struct {
	Char     byte    // 노드의 문자
	FID      int32   // 이 노드에 연결된 FID (0이면 연결된 FID 없음)
	IsEnd    bool    // 이 노드가 geohash의 끝인지 여부
	NumChild int32   // 자식 노드 수
	Children []Child // 자식 노드 배열
}

// Child는 자식 노드에 대한 참조입니다.
type Child struct {
	Char   byte   // 자식 노드의 문자
	Offset uint64 // 파일 내 자식 노드의 오프셋
}

// BinaryTrieIndex는 바이너리 Trie 인덱스 파일을 관리합니다.
type BinaryTrieIndex struct {
	filePath    string
	file        *os.File
	data        []byte
	rootOffset  uint64
	nodeCount   int32
	nextOffset  uint64 // 다음 노드를 저장할 오프셋
	offsetTable map[*TrieNode]uint64 // 메모리 노드와 파일 오프셋 매핑
}

// NewBinaryTrieIndex는 새로운 BinaryTrieIndex를 생성합니다.
func NewBinaryTrieIndex(filePath string) *BinaryTrieIndex {
	return &BinaryTrieIndex{
		filePath:    filePath,
		offsetTable: make(map[*TrieNode]uint64),
	}
}

// Save는 Trie를 바이너리 파일로 저장합니다.
func (b *BinaryTrieIndex) Save(trie *Trie) error {
	// 파일 생성
	file, err := os.Create(b.filePath)
	if err != nil {
		return fmt.Errorf("인덱스 파일을 생성할 수 없습니다: %v", err)
	}
	defer file.Close()

	// 초기화
	b.file = file
	b.nextOffset = FILE_HEADER_SIZE
	b.nodeCount = 0
	b.offsetTable = make(map[*TrieNode]uint64)

	// 파일 헤더 공간 확보 (나중에 채움)
	header := make([]byte, FILE_HEADER_SIZE)
	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("헤더를 쓸 수 없습니다: %v", err)
	}

	// Trie를 재귀적으로 저장
	if trie.Root != nil {
		b.rootOffset, err = b.saveNode(file, trie.Root)
		if err != nil {
			return err
		}
	} else {
		b.rootOffset = 0
	}

	// 파일 헤더 업데이트
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("헤더 위치로 이동할 수 없습니다: %v", err)
	}

	// 매직 넘버 쓰기
	if err := binary.Write(file, binary.LittleEndian, uint32(MAGIC_NUMBER)); err != nil {
		return fmt.Errorf("매직 넘버를 쓸 수 없습니다: %v", err)
	}

	// 버전 쓰기
	if err := binary.Write(file, binary.LittleEndian, uint32(FILE_VERSION)); err != nil {
		return fmt.Errorf("버전을 쓸 수 없습니다: %v", err)
	}

	// 루트 노드 오프셋 쓰기
	if err := binary.Write(file, binary.LittleEndian, b.rootOffset); err != nil {
		return fmt.Errorf("루트 오프셋을 쓸 수 없습니다: %v", err)
	}

	// 노드 수 쓰기
	if err := binary.Write(file, binary.LittleEndian, b.nodeCount); err != nil {
		return fmt.Errorf("노드 수를 쓸 수 없습니다: %v", err)
	}

	log.Printf("바이너리 Trie 인덱스가 저장되었습니다. 노드 수: %d, 파일 크기: %d 바이트\n", 
		b.nodeCount, b.nextOffset)
	return nil
}

// saveNode는 Trie 노드를 바이너리 파일에 재귀적으로 저장합니다.
func (b *BinaryTrieIndex) saveNode(file *os.File, node *TrieNode) (uint64, error) {
	// 이미 저장된 노드인지 확인
	if offset, exists := b.offsetTable[node]; exists {
		return offset, nil
	}

	// 현재 노드의 오프셋 저장
	nodeOffset := b.nextOffset
	b.offsetTable[node] = nodeOffset
	b.nodeCount++

	// 자식 노드 목록 구성
	children := make([]Child, 0, len(node.Children))
	for char := range node.Children {
		children = append(children, Child{
			Char:   char,
			Offset: 0, // 임시로 0 설정, 나중에 업데이트
		})
	}

	// 노드 헤더 크기 계산
	nodeSize := NODE_HEADER_SIZE + CHILD_POINTER_SIZE*len(children)
	b.nextOffset += uint64(nodeSize)

	// 노드 헤더 쓰기
	if _, err := file.Seek(int64(nodeOffset), 0); err != nil {
		return 0, fmt.Errorf("노드 위치로 이동할 수 없습니다: %v", err)
	}

	// 문자 쓰기 (TrieNode에는 Char 필드가 없으므로 0 바이트 사용)
	if err := binary.Write(file, binary.LittleEndian, byte(0)); err != nil {
		return 0, fmt.Errorf("문자를 쓸 수 없습니다: %v", err)
	}
	
	// IsEnd 플래그 쓰기 (1바이트로 표현)
	isEnd := byte(0)
	if node.IsEnd {
		isEnd = 1
	}
	if err := binary.Write(file, binary.LittleEndian, isEnd); err != nil {
		return 0, fmt.Errorf("IsEnd 플래그를 쓸 수 없습니다: %v", err)
	}

	// FID 쓰기
	if err := binary.Write(file, binary.LittleEndian, int32(node.FID)); err != nil {
		return 0, fmt.Errorf("FID를 쓸 수 없습니다: %v", err)
	}

	// 자식 수 쓰기
	if err := binary.Write(file, binary.LittleEndian, int32(len(children))); err != nil {
		return 0, fmt.Errorf("자식 수를 쓸 수 없습니다: %v", err)
	}

	// 자식 포인터 위치 저장
	childrenPos, err := file.Seek(0, 1)
	if err != nil || childrenPos < 0 {
		return 0, fmt.Errorf("현재 위치를 확인할 수 없습니다: %v", err)
	}

	// 자식 포인터 공간 확보 (나중에 채움)
	childrenData := make([]byte, CHILD_POINTER_SIZE*len(children))
	if _, err := file.Write(childrenData); err != nil {
		return 0, fmt.Errorf("자식 포인터 공간을 확보할 수 없습니다: %v", err)
	}

	// 자식 노드 저장 및 포인터 업데이트
	for i, child := range children {
		// 자식 노드 저장
		childOffset, err := b.saveNode(file, node.Children[child.Char])
		if err != nil {
			return 0, err
		}

		// 자식 포인터 업데이트
		if _, err := file.Seek(childrenPos+int64(i*CHILD_POINTER_SIZE), 0); err != nil {
			return 0, fmt.Errorf("자식 포인터 위치로 이동할 수 없습니다: %v", err)
		}

		// 자식 문자 쓰기
		if err := binary.Write(file, binary.LittleEndian, child.Char); err != nil {
			return 0, fmt.Errorf("자식 문자를 쓸 수 없습니다: %v", err)
		}

		// 자식 오프셋 쓰기
		if err := binary.Write(file, binary.LittleEndian, childOffset); err != nil {
			return 0, fmt.Errorf("자식 오프셋을 쓸 수 없습니다: %v", err)
		}
	}

	return nodeOffset, nil
}

// Load는 바이너리 Trie 인덱스 파일을 메모리에 매핑합니다.
func (b *BinaryTrieIndex) Load() error {
	// 파일 열기
	file, err := os.Open(b.filePath)
	if err != nil {
		return fmt.Errorf("인덱스 파일을 열 수 없습니다: %v", err)
	}
	b.file = file

	// 파일 크기 확인
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("파일 정보를 가져올 수 없습니다: %v", err)
	}
	size := fileInfo.Size()
	if size < FILE_HEADER_SIZE {
		file.Close()
		return fmt.Errorf("파일이 너무 작습니다: %v", size)
	}

	// 파일을 메모리에 매핑
	b.data, err = syscall.Mmap(
		int(file.Fd()),
		0,
		int(size),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return fmt.Errorf("파일을 메모리에 매핑할 수 없습니다: %v", err)
	}

	// 헤더 검증
	magic := binary.LittleEndian.Uint32(b.data[0:4])
	if magic != MAGIC_NUMBER {
		b.Close()
		return fmt.Errorf("잘못된 파일 형식입니다: %v", magic)
	}

	version := binary.LittleEndian.Uint32(b.data[4:8])
	if version != FILE_VERSION {
		b.Close()
		return fmt.Errorf("지원되지 않는 파일 버전입니다: %v", version)
	}

	// 루트 노드 오프셋 및 노드 수 읽기
	b.rootOffset = binary.LittleEndian.Uint64(b.data[8:16])
	b.nodeCount = int32(binary.LittleEndian.Uint32(b.data[16:20]))

	log.Printf("바이너리 Trie 인덱스가 로드되었습니다. 노드 수: %d, 파일 크기: %d 바이트\n", 
		b.nodeCount, size)
	return nil
}

// Close는 바이너리 Trie 인덱스 파일을 닫습니다.
func (b *BinaryTrieIndex) Close() error {
	if b.data != nil {
		if err := syscall.Munmap(b.data); err != nil {
			return fmt.Errorf("메모리 매핑을 해제할 수 없습니다: %v", err)
		}
		b.data = nil
	}

	if b.file != nil {
		if err := b.file.Close(); err != nil {
			return fmt.Errorf("파일을 닫을 수 없습니다: %v", err)
		}
		b.file = nil
	}

	return nil
}

// SearchGeohash는 매핑된 바이너리 Trie 인덱스에서 geohash에 대한 FID를 검색합니다.
func (b *BinaryTrieIndex) SearchGeohash(geohash string) (int, error) {
	if b.data == nil {
		return 0, fmt.Errorf("인덱스가 로드되지 않았습니다")
	}

	if b.rootOffset == 0 {
		return 0, nil // 빈 Trie
	}

	// 루트 노드부터 시작
	currentOffset := b.rootOffset
	bestFID := 0

	// geohash의 각 문자에 대해 Trie 탐색
	for i := 0; i < len(geohash); i++ {
		char := geohash[i]

		// 현재 노드 읽기
		if currentOffset >= uint64(len(b.data)) {
			return 0, fmt.Errorf("잘못된 노드 오프셋: %v", currentOffset)
		}

		// 노드 헤더 읽기
		_ = b.data[currentOffset] // nodeChar (사용하지 않음)
		isEnd := b.data[currentOffset+1] == 1 // IsEnd 플래그
		nodeFID := int(binary.LittleEndian.Uint32(b.data[currentOffset+2:currentOffset+6]))
		numChild := int(binary.LittleEndian.Uint32(b.data[currentOffset+6:currentOffset+10]))

		// FID 업데이트 (노드에 FID가 있고 IsEnd가 true인 경우)
		if nodeFID > 0 && isEnd {
			bestFID = nodeFID
		}

		// 자식 노드 찾기
		childOffset := uint64(0)
		found := false

		// 자식 포인터 배열 시작 위치
		childrenStart := currentOffset + NODE_HEADER_SIZE

		// 자식 노드 검색
		for j := 0; j < numChild; j++ {
			childPos := childrenStart + uint64(j*CHILD_POINTER_SIZE)
			childChar := b.data[childPos]
			
			if childChar == char {
				childOffset = binary.LittleEndian.Uint64(b.data[childPos+1:childPos+9])
				found = true
				break
			}
		}

		// 매칭되는 자식이 없으면 현재까지의 최선의 FID 반환
		if !found {
			break
		}

		// 다음 노드로 이동
		currentOffset = childOffset
	}

	// 마지막 노드 확인 (마지막 문자까지 매칭된 경우)
	if currentOffset < uint64(len(b.data)) {
		isEnd := b.data[currentOffset+1] == 1
		nodeFID := int(binary.LittleEndian.Uint32(b.data[currentOffset+2:currentOffset+6]))
		
		if nodeFID > 0 && isEnd {
			bestFID = nodeFID
		}
	}

	return bestFID, nil
}
