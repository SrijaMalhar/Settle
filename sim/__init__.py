"""A deterministic simulator: the real engine on one thread, a fake clock, PSPs that know the
truth, and fault switches. The same seed gives the same run, checked by a hash of the trace."""
