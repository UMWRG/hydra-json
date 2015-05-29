pyinstaller -y --upx-dir=../../upx ExportJSON.spec
xcopy dist\ExportJSON\* ..\JSONPlugin\plugins\Export\ /Y /E
copy plugin.xml ..\JSONPlugin\plugins\Export\
copy icon16.png ..\JSONPlugin\plugins\Export\
copy icon32.png ..\JSONPlugin\plugins\Export\
PAUSE
