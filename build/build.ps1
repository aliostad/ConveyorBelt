param(
    $buildFile   = (join-path (Split-Path -parent $MyInvocation.MyCommand.Definition) "ConveyorBelt.msbuild"),
    $buildParams = "/p:Configuration=Release",
    $buildTarget = "/t:Default",
	$vsVersion   = "14.0"
)

& "$(get-content env:windir)\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe" $buildFile ($buildParams + ";VisualStudioVersion=$vsVersion") $buildTarget