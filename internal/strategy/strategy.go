// Package strategy provides pluggable routing algorithm implementations
// for the GopherLoad load balancer.
//
// Each strategy in this package implicitly satisfies the balancer.Strategy
// interface via Go's structural typing. No local Strategy interface is
// defined here — balancer.Strategy is the single source of truth for the
// routing contract.
package strategy
