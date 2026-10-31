$env:JAVA_HOME = 'C:\Users\mursa\AppData\Local\Programs\Temurin17\PFiles64\Eclipse Adoptium\jdk-17.0.20.101-hotspot'
$env:PATH = $env:JAVA_HOME + '\bin;' + $env:PATH
& 'C:\Users\mursa\Downloads\DistributedKVStore\scripts\verify-long-soak.ps1' -DurationHours 24 -RequireEveryTestPresent -FullSuiteEveryCycles 6
