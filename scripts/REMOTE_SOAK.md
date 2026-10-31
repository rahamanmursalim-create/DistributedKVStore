# Fresh Linux VM soak run

The Windows soak process cannot be migrated or resumed. Transfer this source bundle and start a new full 24-hour run. The bundle excludes `target/`, active IDE output, and prior run artifacts; it contains the exact current `pom.xml`, Java sources/tests, `legacy/`, and soak scripts.

## 1. Copy the source bundle

From the repository root on Windows:

```powershell
Get-FileHash .\artifacts\distributedkvstore-remote-soak-source.zip -Algorithm SHA256
scp .\artifacts\distributedkvstore-remote-soak-source.zip ubuntu@VM_HOST:/tmp/
```

Compare the printed hash with the VM's `sha256sum` before extracting:

```bash
sha256sum /tmp/distributedkvstore-remote-soak-source.zip
```

Do not proceed if the hashes differ.

## 2. Prepare the VM

On a fresh Ubuntu VM:

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk maven unzip python3 util-linux
if ! getent passwd kvstore >/dev/null; then sudo useradd --system --create-home --home-dir /var/lib/kvstore kvstore; fi
sudo mkdir -p /opt/distributed-kvstore /var/lib/kvstore-soak/artifacts
sudo unzip -o /tmp/distributedkvstore-remote-soak-source.zip -d /opt/distributed-kvstore
sudo chown -R kvstore:kvstore /opt/distributed-kvstore /var/lib/kvstore-soak
sudo chmod 0755 /opt/distributed-kvstore/scripts/run-24h-soak.sh
```

Verify the transferred archive checksum before extracting it. Run the following preliminary checks as the service user; **do not start the 24-hour service unless every command succeeds**:

```bash
sudo -u kvstore bash -c '
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  export PATH="$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  cd /opt/distributed-kvstore
  bash -n scripts/run-24h-soak.sh
  python3 -m py_compile scripts/check-classfile-version.py
  java -version
  mvn -version
  mvn -B "-Daether.syncContext.named.factory=rwlock-local" clean test
  python3 scripts/check-classfile-version.py target/classes target/test-classes
'
```

The preliminary build must show 45 JUnit tests passing, Maven running on Java 17, and class-file major version 61 for every generated class. Use a fresh checkout with no IDE, Maven, Gradle, or javac process writing into it.

## 3. Start the detached systemd run

The example unit uses the Ubuntu OpenJDK 17 path. Install and start it manually; do not enable it at boot, because a reboot must not be mistaken for an uninterrupted 24-hour run:

```bash
sudo install -m 0644 /opt/distributed-kvstore/scripts/kvstore-soak.service.example /etc/systemd/system/kvstore-soak.service
sudo systemctl daemon-reload
sudo systemctl start kvstore-soak.service
sudo systemctl status kvstore-soak.service --no-pager
sudo journalctl -u kvstore-soak.service -f
```

The SSH session can close after `systemctl start` returns. Keep the VM running and prevent scheduled shutdowns. The runner records a UTC timestamp and exit code for each full `mvn clean test`, verifies all output classes are Java 17, fails the run on the first failed cycle, and updates `summary.txt` after each cycle.

## 4. Collect and verify results

After the service finishes:

```bash
sudo systemctl status kvstore-soak.service --no-pager
sudo cat /var/lib/kvstore-soak/artifacts/kvstore-soak-*/summary.txt
sudo tar -C /var/lib/kvstore-soak -czf /tmp/kvstore-soak-evidence.tar.gz artifacts
sha256sum /tmp/kvstore-soak-evidence.tar.gz
sha256sum /tmp/kvstore-soak-evidence.tar.gz > /tmp/kvstore-soak-evidence.tar.gz.sha256
```

Copy the archive and checksum back to Windows:

```powershell
scp ubuntu@VM_HOST:/tmp/kvstore-soak-evidence.tar.gz .\artifacts\
scp ubuntu@VM_HOST:/tmp/kvstore-soak-evidence.tar.gz.sha256 .\artifacts\
```

Verify the downloaded evidence archive using the sidecar (`sha256sum -c kvstore-soak-evidence.tar.gz.sha256`). A PASS requires `status=PASS`, `durationSatisfied=true`, no failed cycles, all expected tests present, and at least 86,400 elapsed seconds. An interrupted, rebooted, or failed run is incomplete and must be restarted from zero.
