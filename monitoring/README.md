# Prometheus and Alertmanager

These files are deployment examples, not proof of a production monitoring rollout.
Set real node addresses, install the client token through the Prometheus secret
mechanism, configure the private CA when needed, and replace the Alertmanager
webhook with the approved notification gateway before deployment. Keep secrets
out of this repository and dashboards.

The Prometheus rules use the application metrics described in the root README.
`up{job="distributed-kvstore"}` also catches a missing scrape target; the
`kv_process_up` metric catches an unhealthy process whose metrics endpoint still
responds. Key counts are local/replica duplicated in Raft mode and are not a
cluster-wide unique-key count.

Validate the configuration with the same pinned Prometheus/Alertmanager releases
used in deployment:

```bash
promtool check config prometheus.yml
promtool check rules kvstore-alerts.yml
amtool check-config alertmanager.yml
```

Run the local synthetic notification/clearing check with Docker available:

```powershell
python .\scripts\validate-alertmanager-delivery.py --artifacts artifacts
```

It starts isolated Prometheus, Alertmanager, a synthetic metrics target, and a
local webhook receiver; it induces target loss, waits for a firing notification,
restores the target, and waits for the resolved notification. It cleans up its
own containers and network and saves configs, notifications, logs, and a summary
under `artifacts/`. The synthetic test must not be reported as production
notification validation.

For a deployment acceptance test, trigger one alert from a dedicated test node,
confirm the expected receiver and on-call acknowledgement, restore the node,
then confirm a resolved notification. Preserve Prometheus alert state, the
Alertmanager notification/receiver logs, and the on-call acknowledgement. A
successful config check or local synthetic webhook test does not prove delivery
to the production notification channel.
