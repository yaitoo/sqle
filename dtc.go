package sqle

import (
	"context"
	"database/sql"
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
func (d *DTC) Commit() error {
	for _, s := range d.sessions {
		tx, err := s.client.BeginTx(d.ctx, d.opts)
		if err != nil {
			return err
		}

		s.tx = tx

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
func (d *DTC) Rollback() []error {
	var errs []error

	for _, s := range d.sessions {
		if s.committed {
			for _, revert := range s.revert {
				if err := revert(d.ctx, s.client); err != nil {
					errs = append(errs, err)
				}
			}

		} else {
			if err := s.tx.Rollback(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errs
}
