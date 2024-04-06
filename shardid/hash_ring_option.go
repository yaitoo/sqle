package shardid

type HashRingOption func(r *HashRing)

func WithReplicas(nodes ...string) HashRingOption {
	return func(r *HashRing) {
		r.replicas = nodes
	}
}
