package sqle

import (
	"context"
	"database/sql"
	"errors"
)

// DTC Distributed Transaction Coordinator
type DTC struct {
	ctx  context.Context
	opts *sql.TxOptions

	sessions []*session
}

// session represents a transaction session.
type session struct {
	committed bool
	client    *Client
	tx        *Tx
	exec      []func(context.Context, Connector) error
	revert    []func(context.Context, Connector) error
}

// NewDTC creates a new instance of DTC.
func NewDTC(ctx context.Context, opts *sql.TxOptions) *DTC {
	return &DTC{
		ctx:  ctx,
		opts: opts,
	}
}

// Prepare adds a new transaction session to the DTC.
func (d *DTC) Prepare(client *Client, exec func(ctx context.Context, conn Connector) error, revert func(ctx context.Context, conn Connector) error) {
	for _, s := range d.sessions {
		if s.client == client {
			s.exec = append(s.exec, exec)
			s.revert = append(s.revert, revert)
			return
		}
	}

	s := &session{
		committed: false,
		client:    client,
		exec: []func(ctx context.Context, c Connector) error{
			exec,
		},
		revert: []func(ctx context.Context, c Connector) error{
			revert,
		},
	}

	d.sessions = append(d.sessions, s)

}

// Commit commits all the prepared transactions in the DTC.
//
// On any error returned from BeginTx or any exec callback, Commit rolls back
// every transaction it has already begun in this call before returning. This
// prevents leaking half-open transactions whose rows would otherwise remain
// uncommitted and whose connections would be held until GC. Any error
// encountered while rolling back is joined onto the returned error so the
// caller can see it.
//
// Note: this rollback-on-failure covers only the BeginTx and exec phases.
// If a session has already been Commit()ed when a later Commit fails, its
// rows are persisted; compensating via the revert callbacks is the caller's
// responsibility (typically by calling Rollback after Commit returns an
// error).
func (d *DTC) Commit() (err error) {
	opened := make([]*session, 0, len(d.sessions))
	defer func() {
		if err != nil {
			for _, s := range opened {
				if s.committed {
					continue
				}
				if rbErr := s.tx.Rollback(); rbErr != nil {
					err = errors.Join(err, rbErr)
				}
				// Drop the rolled-back tx so a subsequent Rollback call
				// (following the documented commit-then-rollback pattern)
				// does not re-issue Rollback and collect spurious ErrTxDone.
				s.tx = nil
			}
		}
	}()

	for _, s := range d.sessions {
		tx, err := s.client.BeginTx(d.ctx, d.opts)
		if err != nil {
			return err
		}

		s.tx = tx
		opened = append(opened, s)

		for _, exec := range s.exec {
			err = exec(d.ctx, tx)
			if err != nil {
				return err
			}
		}
	}

	for _, s := range d.sessions {
		err := s.tx.Commit()
		if err != nil {
			return err
		}

		s.committed = true
	}

	return nil
}

// Rollback rolls back all the prepared transactions in the DTC.
//
// For each session that has been committed, the registered revert callbacks
// are invoked against the underlying Client (compensating actions). For each
// session that has begun a transaction but not committed it, the transaction
// is rolled back. Sessions whose BeginTx failed and therefore have no
// transaction are skipped.
func (d *DTC) Rollback() []error {
	var errs []error

	for _, s := range d.sessions {
		if s.committed {
			for _, revert := range s.revert {
				if err := revert(d.ctx, s.client); err != nil {
					errs = append(errs, err)
				}
			}

		} else if s.tx != nil {
			if err := s.tx.Rollback(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errs
}
