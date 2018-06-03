param(
    $buildFile   = (join-path (Split-Path -parent $MyInvocation.MyCommand.Definition) "ConveyorBelt.msbuild"),
    $buildParams = "/p:Configuration=Release;azureTargetsLoc=C:\Program Files (x86)\MSBuild\Microsoft\VisualStudio\v14.0\Windows Azure Tools\2.9",
    $buildTarget = "/t:Default",
	$msbuildFile = "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\MSBuild\15.0\Bin\MSBuild.exe",
	$vsVersion   = "14.0"
)

& $msbuildFile $buildFile ($buildParams + ";VisualStudioVersion=$vsVersion") $buildTarget