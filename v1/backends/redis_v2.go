package backends

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/liticer/machinery/v1/common"
	"github.com/liticer/machinery/v1/config"
	"github.com/liticer/machinery/v1/log"
	"github.com/liticer/machinery/v1/tasks"
	"github.com/RichardKnop/redsync"
	"time"
)

// RedisV2Backend represents a Redis result backend
type RedisV2Backend struct {
	reader  common.RedisClientInterface
	writer  common.RedisClientInterface
	redsync *redsync.Redsync
	Backend
}

// NewRedisBackend creates RedisV2Backend instance
func NewRedisV2Backend(cnf *config.Config, reader common.RedisClientInterface, writer common.RedisClientInterface) Interface {
	return &RedisV2Backend{ Backend: New(cnf), reader:  reader, writer: writer}
}

// InitGroup creates and saves a group meta data object
func (b *RedisV2Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	return b.writer.Set(groupUUID, encoded, b.getExpirationTime()).Err()
}

// GroupCompleted returns true if all tasks in a group finished
func (b *RedisV2Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *RedisV2Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *RedisV2Backend) TriggerChord(groupUUID string) (bool, error) {

	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	if err = b.writer.Set(groupUUID, encoded, 0).Err(); err != nil {
		return false, err
	}

	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *RedisV2Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *RedisV2Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *RedisV2Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *RedisV2Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *RedisV2Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *RedisV2Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *RedisV2Backend) GetState(taskUUID string) (*tasks.TaskState, error) {

	cmd := b.reader.Get(taskUUID)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	item, err := cmd.Bytes()
	if err != nil {
		return nil, cmd.Err()
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *RedisV2Backend) PurgeState(taskUUID string) error {
	return b.writer.Del(taskUUID).Err()
}

// PurgeGroupMeta deletes stored group meta data
func (b *RedisV2Backend) PurgeGroupMeta(groupUUID string) error {
	return b.writer.Del(groupUUID).Err()
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *RedisV2Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	cmd := b.reader.Get(groupUUID)
	item, err := cmd.Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *RedisV2Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		taskUUIDInterfaces[i] = interface{}(taskUUID)
	}

	cmd := b.reader.MGet(taskUUIDs...)
	if cmd.Err() != nil {
		return taskStates, cmd.Err()
	}

	for i, value := range cmd.Val() {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("expected byte array, instead got: %v", value)
		}

		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			log.ERROR.Print(err)
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *RedisV2Backend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	return b.writer.Set(taskState.TaskUUID, encoded, b.getExpirationTime()).Err()
}

// getExpirationTime return expiration time duration for a stored task state
func (b *RedisV2Backend) getExpirationTime() time.Duration {
	expiresIn := b.cnf.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	return time.Duration(expiresIn) * time.Second
}
