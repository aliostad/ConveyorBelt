param(
    $buildFile   = (join-path (Split-Path -parent $MyInvocation.MyCommand.Definition) "ConveyorBelt.msbuild"),
    $buildParams = "/p:Configuration=Release;VisualStudioVersion=14.0",
    $buildTarget = "/t:Default"
)

& "$(get-content env:windir)\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe" $buildFile $buildParams $buildTarget