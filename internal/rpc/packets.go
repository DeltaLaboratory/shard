package rpc

import (
	"encoding/binary"
	"fmt"
	"time"
)

type OperationID uint64

type Operation interface {
	Marshal() (OperationID, []byte, error)
}

func MarshalPacket(op Operation) (OperationID, []byte, error) {
	operationID, operationBody, err := op.Marshal()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to marshal operation: %w", err)
	}

	packet := make([]byte, 8+8+8+len(operationBody))
	binary.BigEndian.PutUint64(packet, uint64(operationID))
	binary.BigEndian.PutUint64(packet[8:], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(packet[16:], uint64(len(operationBody)))
	copy(packet[8:], operationBody)

	return operationID, packet, nil
}

func UnmarshalPacket(packet []byte) (OperationID, time.Time, []byte, error) {
	if len(packet) < 24 {
		return 0, time.Time{}, nil, fmt.Errorf("packet is too short")
	}

	operationID := OperationID(binary.BigEndian.Uint64(packet))
	timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(packet[8:])))
	operationBodyLength := binary.BigEndian.Uint64(packet[16:])
	operationBody := packet[24:]

	if uint64(len(operationBody)) != operationBodyLength {
		return 0, time.Time{}, nil, fmt.Errorf("operation body length mismatch")
	}

	return operationID, timestamp, operationBody, nil
}
