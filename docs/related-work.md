# Related systems and contribution boundary

Verified primary references (checked 2026-10-11):

- [Git log documentation](https://git-scm.com/docs/git-log): history, parent traversal
  and timestamp formats are established tooling. Configuration versioning alone is
  not a novel contribution.
- [Oxidized](https://github.com/ytti/oxidized), a network configuration backup system
  with Git output/history: configuration backup and version comparison already exist.
  ConfigStream does not replace collection/backup and adds no SSH collector here.
- [Batfish configuration validation](https://batfish.readthedocs.io/en/stable/notebooks/linked/validating-configuration-settings.html): vendor-independent network
  analysis/validation differs from ConfigStream's conservative text normalization,
  version pairing and regex rules. No equivalent network-impact guarantee is claimed.
- [OpenConfig gNMI specification](https://github.com/openconfig/reference/blob/master/rpc/gnmi/gnmi-specification.md): configuration access and streaming telemetry are
  distinct capabilities. Telemetry frequency must not be reused as a configuration
  change-rate statistic; gNMI is not implemented by ConfigStream.
- [Puppet control repositories](https://www.puppet.com/docs/pe/2023.5/control_repo.html):
  configuration-as-code/version-controlled management is established practice.
  ConfigStream does not implement deployment orchestration or enforce desired state.

The university contribution is a **reproducible integrated data pipeline**, native
version-aware stream state with out-of-order pairing, committed historical analytics,
safe derived serving and empirically characterized/calibrated workload evaluation.
These references support architectural context, not novelty from simply retaining
versions or unsupported industry-scale/change-frequency claims.
