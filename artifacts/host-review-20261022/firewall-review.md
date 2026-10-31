# Local host firewall review

Date: 2026-10-22 (local time, Asia/Calcutta)

This is a read-only review of the Windows host running this checkout. It is not
a review of a separately deployed cluster or of an upstream network firewall.

## Effective local observations

- Windows Defender Firewall Domain, Private, and Public profiles are enabled.
- Each profile reports `DefaultInboundAction=NotConfigured` in the queried
  ActiveStore view.
- No enabled inbound rule for this application was identified for its client
  ports (6379/8080) or the Docker test's published ports (16379/18081).
- Port 7000 has enabled TCP and UDP allow rules on Public and Private profiles
  with `RemoteAddress=Any`. Their programs are Windows system applications
  `WindowsBackupClient.exe` and `OobeHostApp.exe`; the rules are local
  PersistentStore entries, not rules for this Java service. They were left
  unchanged.
- Wi-Fi address observed during this review: `10.160.111.94`. No separate
  reachable cluster host was identified. This does not verify inbound
  reachability from another machine.

## Limits

This review does not prove provider, router, VPN, corporate, or deployed-host
firewall policy. It does not establish the effective default action from an
external vantage point. The broad OS rules above should be reviewed by the
machine administrator if they are unexpected; this task did not alter them.

The Docker cluster TLS rotation check is recorded separately in
`../docker-cluster-20261022-144638/summary.json`; it is a same-host container
test and is not production certificate deployment evidence.
