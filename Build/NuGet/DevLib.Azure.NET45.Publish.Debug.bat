@echo off

set "PROJ=DevLib.Azure.NET45"
set "CONFIG=Debug"

@echo --------------------------------
@echo NuGet start to pack...

nuget pack "..\..\Source\%PROJ%\%PROJ%.csproj" -Properties "Configuration=%CONFIG%" -OutputDirectory "..\..\Source\%PROJ%\bin\%CONFIG%"


@echo --------------------------------
@echo List nupkg files...

dir "..\..\Source\%PROJ%\bin\%CONFIG%\*.nupkg"

for /f "delims=" %%x in ('dir /od /b "..\..\Source\%PROJ%\bin\%CONFIG%\*.nupkg"') do set LATEST=%%x

if %Errorlevel% NEQ 0 goto End


@echo --------------------------------

choice /M "Do you want to publish %LATEST% ?"

if Errorlevel 2 goto No
if Errorlevel 1 goto Yes

goto End

:No
goto End

:Yes
@echo nuget push "..\..\Source\%PROJ%\bin\%CONFIG%\%LATEST%" %1-Source https://api.nuget.org/v3/index.json -Verbosity detailed
nuget push "..\..\Source\%PROJ%\bin\%CONFIG%\%LATEST%" %1-Source https://api.nuget.org/v3/index.json -Verbosity detailed

:End
@echo --------------------------------
@echo Done!