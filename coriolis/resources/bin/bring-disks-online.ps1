function Convert-RawDetailsToHashmap {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [object]$RawData
    )

    $details = @{}

    foreach($i in $RawData) {
        if ($i.Contains(":")) {
            $kv = $i.Split(":", 2)
            if ($kv.Count -ne 2) {
                continue
            }
            $details[$kv[0].Trim()] = $kv[1].Trim()
        }
    }
    return $details
}

function Invoke-DiskpartScript {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [String]$Script
    )

    $scriptName = Join-Path $env:TMP ("diskpart-script-{0}.txt" -f (Get-Random))
    Set-Content $scriptName $Script
    $rawDetails = diskpart /s $scriptName
    if ($LASTEXITCODE) {
        rm -Force $scriptName
        Throw "Failed to run diskpart $LASTEXITCODE"
    }
    rm $scriptName
    return $rawDetails
}

function Get-DiskDetails {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
        [Int]$DiskIndex
    )

    $disks = gcim -ClassName Win32_DiskDrive | Where-Object {$_.Index -eq $DiskIndex}
    if (!$disks){
        Throw "Cannot find disk with index $DiskIndex"
    }

    $rawDetailsScript = "select disk {0}`ndetail disk" -f $DiskIndex
    $rawDetails = Invoke-DiskpartScript $rawDetailsScript
    $details = Convert-RawDetailsToHashmap $rawDetails
    $details["Index"] = $DiskIndex
    return $details
}

function Set-DiskOnline {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
        [hashtable]$DiskDetails
    )

    if ($DiskDetails["Status"].Contains("Offline")) {
        $onlineScript = "select disk {0}`nonline disk" -f $DiskDetails["Index"]
        Invoke-DiskpartScript $onlineScript | Out-Null
    }
    return Get-DiskDetails $DiskDetails["Index"]
}

function Import-ForeignDisk {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true, ValueFromPipeline=$true)]
        [hashtable]$DiskDetails
    )

    if($DiskDetails["Status"] -ne "Foreign") {
        return
    }

    if($DiskDetails["Current Read-only State"] -eq "Yes" -or $DiskDetails["Read-only"] -eq "Yes") {
        $setRW = "SELECT DISK {0}`nATTRIBUTES DISK CLEAR READONLY" -f $DiskDetails["Index"]
        Invoke-DiskpartScript $setRW | Out-Null
    }

    $importScript = "select disk {0}`nIMPORT" -f $DiskDetails["Index"]
    Invoke-DiskpartScript $importScript | Out-Null

    return Get-DiskDetails $DiskDetails["Index"]
}

function Invoke-Main {
    $disks = (gcim -ClassName Win32_DiskDrive).Index
    foreach($dsk in $disks) {
        Get-DiskDetails $dsk | Set-DiskOnline | Import-ForeignDisk | Out-Null
    }
    Get-Disk | Set-Disk -IsOffline:$false -ErrorAction 'SilentlyContinue'
    Get-Disk | Where-Object {$_.IsBoot -eq $false} | Set-Disk -IsReadOnly:$false -ErrorAction 'SilentlyContinue'
}

Invoke-Main
Start-Sleep 60
Invoke-Main
