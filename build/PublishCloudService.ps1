Param(  $serviceName = "",
        $storageAccountName = "",
        $packageLocation = "",
        $cloudConfigLocation = "",
        $environment = "Production",
        $deploymentLabel = "Development",
        $timeStampFormat = "g",
        $alwaysDeleteExistingDeployments = 1,
        $enableDeploymentUpgrade = 1,
        $selectedsubscription = "",
        $subscriptionDataFile = "cloud.publishsettings",
        $slot = "Production",
        $affinityGroupName = "",
        $tokensFile = "tokensMain.json"
     )


function Package()
{

   . $env:windir\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe 
        /target:Publish `
        /p:VisualStudioVersion=12.0;Configuration=Release;TargetProfile=Cloud `


}

function Publish()
{
    $deployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot -ErrorVariable a -ErrorAction silentlycontinue 
    if ($a[0] -ne $null)
    {
        Write-Output "$(Get-Date –f $timeStampFormat) - No deployment is detected. Creating a new deployment. "
    }
    #check for existing deployment and then either upgrade, delete + deploy, or cancel according to $alwaysDeleteExistingDeployments and $enableDeploymentUpgrade boolean variables
    if ($deployment.Name -ne $null)
    {
        switch ($alwaysDeleteExistingDeployments)
        {
            1 
            {
                switch ($enableDeploymentUpgrade)
                {
                    1  #Update deployment inplace (usually faster, cheaper, won't destroy VIP)
                    {
                        Write-Output "$(Get-Date –f $timeStampFormat) - Deployment exists in $servicename.  Upgrading deployment."
                        UpgradeDeployment
                    }
                    0  #Delete then create new deployment
                    {
                        Write-Output "$(Get-Date –f $timeStampFormat) - Deployment exists in $servicename.  Deleting deployment."
                        DeleteDeployment
                        CreateNewDeployment

                    }
                } # switch ($enableDeploymentUpgrade)
            }
            0
            {
                Write-Output "$(Get-Date –f $timeStampFormat) - ERROR: Deployment exists in $servicename.  Script execution cancelled."
                exit
            }
        } #switch ($alwaysDeleteExistingDeployments)
    } else {
            CreateNewDeployment
    }
}

function CreateNewDeployment()
{
    write-progress -id 3 -activity "Creating New Deployment" -Status "In progress"
    Write-Host "$(Get-Date –f $timeStampFormat) - Creating New Deployment: In progress" -ForegroundColor DarkCyan
    Write-Host "$(Get-Date –f $timeStampFormat) - Service name is $serviceName and config is $cloudConfigLocation" -ForegroundColor green
    
    Write-Host "Creating new service $serviceName ..."
    New-AzureService -ServiceName $serviceName -AffinityGroup $affinityGroupName

    $opstat = New-AzureDeployment -Verbose -Slot $slot -Package $packageLocation -Configuration $cloudConfigLocation -label $deploymentLabel -ServiceName $serviceName

    $completeDeployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot -Verbose
    $completeDeploymentID = $completeDeployment.deploymentid

    write-progress -id 3 -activity "Creating New Deployment" -completed -Status "Complete"
    Write-Output "$(Get-Date –f $timeStampFormat) - Creating New Deployment: Complete, Deployment ID: $completeDeploymentID"

    StartInstances
}

function UpgradeDeployment()
{
    write-progress -id 3 -activity "Upgrading Deployment" -Status "In progress"
    Write-Output "$(Get-Date –f $timeStampFormat) - Upgrading Deployment: In progress"

    # perform Update-Deployment
    $setdeployment = Set-AzureDeployment -Upgrade -Slot $slot -Package $packageLocation -Configuration $cloudConfigLocation -label $deploymentLabel -ServiceName $serviceName -Force

    $completeDeployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot
    $completeDeploymentID = $completeDeployment.deploymentid

    write-progress -id 3 -activity "Upgrading Deployment" -completed -Status "Complete"
    Write-Output "$(Get-Date –f $timeStampFormat) - Upgrading Deployment: Complete, Deployment ID: $completeDeploymentID"
}

function DeleteDeployment()
{
    write-progress -id 2 -activity "Deleting Deployment" -Status "In progress"
    Write-Output "$(Get-Date –f $timeStampFormat) - Deleting Deployment: In progress"

    #WARNING - always deletes with force
    $removeDeployment = Remove-AzureDeployment -Slot $slot -ServiceName $serviceName -Force

    write-progress -id 2 -activity "Deleting Deployment: Complete" -completed -Status $removeDeployment
    Write-Output "$(Get-Date –f $timeStampFormat) - Deleting Deployment: Complete"
}

function StartInstances()
{
    write-progress -id 4 -activity "Starting Instances" -status "In progress"
    Write-Output "$(Get-Date –f $timeStampFormat) - Starting Instances: In progress"

    $deployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot
    $runstatus = $deployment.Status

    if ($runstatus -ne 'Running') 
    {
        $run = Set-AzureDeployment -Slot $slot -ServiceName $serviceName -Status Running
    }
    $deployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot
    $oldStatusStr = @("") * $deployment.RoleInstanceList.Count

    while (-not(AllInstancesRunning($deployment.RoleInstanceList)))
    {
        $i = 1
        foreach ($roleInstance in $deployment.RoleInstanceList)
        {
            $instanceName = $roleInstance.InstanceName
            $instanceStatus = $roleInstance.InstanceStatus

            if ($oldStatusStr[$i - 1] -ne $roleInstance.InstanceStatus)
            {
                $oldStatusStr[$i - 1] = $roleInstance.InstanceStatus
                Write-Output "$(Get-Date –f $timeStampFormat) - Starting Instance '$instanceName': $instanceStatus"
            }

            write-progress -id (4 + $i) -activity "Starting Instance '$instanceName'" -status "$instanceStatus"
            $i = $i + 1
        }

        sleep -Seconds 1

        $deployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot
    }

    $i = 1
    foreach ($roleInstance in $deployment.RoleInstanceList)
    {
        $instanceName = $roleInstance.InstanceName
        $instanceStatus = $roleInstance.InstanceStatus

        if ($oldStatusStr[$i - 1] -ne $roleInstance.InstanceStatus)
        {
            $oldStatusStr[$i - 1] = $roleInstance.InstanceStatus
            Write-Output "$(Get-Date –f $timeStampFormat) - Starting Instance '$instanceName': $instanceStatus"
        }

        $i = $i + 1
    }

    $deployment = Get-AzureDeployment -ServiceName $serviceName -Slot $slot
    $opstat = $deployment.Status 

    write-progress -id 4 -activity "Starting Instances" -completed -status $opstat
    Write-Output "$(Get-Date –f $timeStampFormat) - Starting Instances: $opstat"
}

function AllInstancesRunning($roleInstanceList)
{
    foreach ($roleInstance in $roleInstanceList)
    {
        if ($roleInstance.InstanceStatus -ne "ReadyRole")
        {
            return $false
        }
    }

    return $true
}



function Package([ref] $packagePath)
{


   . $env:windir\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe ..\src\ConveyorBelt\ConveyorBelt.ccproj `
            /target:Publish `
            /p:VisualStudioVersion=12.0`;Configuration=Release`;TargetProfile=Cloud
     $packagePath.Value = ((GetSolutionRootFolder) + "\src\ConveyorBelt\bin\Release\app.publish\ConveyorBelt.cspkg")
     $val = ($packagePath).Value
     Write-Host "Package created at $val" -foregroundcolor "green"
}

function GetCurrentLocation()
{   
    $ScriptPath = $script:MyInvocation.MyCommand.Path
    $ScriptDir  = Split-Path -Parent $ScriptPath
    return $ScriptDir
}

function GetSolutionRootFolder()
{   
    $item = Get-Item (GetCurrentLocation)
    return $item.Parent.FullName
}


function ReplaceTokens([String] $targetFile, [String] $tokensFile)
{
    $targetContent = (Get-Content $targetFile) -join "`n"
    $js = (Get-Content $tokensFile) -join "`n" | ConvertFrom-Json
    foreach($p in $js.PsObject.Properties){
        $targetContent = $targetContent -replace ("###__" + $p.Name + "__###"), $p.Value
    }
    $targetContent > $targetFile

    Write-Host "Replaced tokens in $targetFile" -ForegroundColor DarkCyan
}

Write-Host $serviceName


Write-Host  "Packaging the project" -foregroundcolor "green"
$packagePath = ""
Package ([ref] $packagePath)
Write-Host "So the package is here: $packagePath"
$packageLocation = $packagePath
$outFolder = (get-item $packagePath ).DirectoryName
$cloudConfigLocation = (gci ($outFolder + "\*.cscfg")).FullName
ReplaceTokens $cloudConfigLocation $tokensFile

# specify path for Azure module (anyone knows how to configure PSModuleuPath?)
$env:PSModulePath=$env:PSModulePath+";"+"C:\Program Files (x86)\Microsoft SDKs\Windows Azure\PowerShell"

# append timestamp to deployment label:
$deploymentLabel=$deploymentLabel + " " + (Get-Date -f g)

# it should list Azure in available modules list:
Get-Module -ListAvailable

#configure powershell with Azure xxx modules
Import-Module Azure

# configure powershell with publishsettings for your subscription
$pubsettings = $subscriptionDataFile
Import-AzurePublishSettingsFile $pubsettings
Select-AzureSubscription -SubscriptionName $selectedsubscription

Write-Host "Storage account name is $storageAccountName" -ForegroundColor Yellow
Set-AzureSubscription -CurrentStorageAccount $storageAccountName -SubscriptionName $selectedsubscription

# set remaining environment variables for Azure cmdlets
$subscription = Get-AzureSubscription $selectedsubscription
$subscriptionname = $subscription.subscriptionname
$subscriptionid = $subscription.subscriptionid
$slot = $environment

# main driver - publish & write progress to activity log
Write-Output "$(Get-Date –f $timeStampFormat) - Azure Cloud Service deploy script started."
Write-Output "$(Get-Date –f $timeStampFormat) - Preparing deployment of $deploymentLabel for $subscriptionname with Subscription ID $subscriptionid."

Publish

$deployment = Get-AzureDeployment -slot $slot -serviceName $servicename
$deploymentUrl = $deployment.Url

Write-Output "$(Get-Date –f $timeStampFormat) - Azure Cloud Service deploy script finished."
Write-Output "$(Get-Date –f $timeStampFormat) - Created Cloud Service with URL: "
Write-Output "$deploymentUrl"
exit 0