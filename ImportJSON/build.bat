pyinstaller -y --upx-dir=../../upx ImportJSON.spec
xcopy dist\ImportJSON\* ..\JSONPlugin\plugins\Import\ /Y /E
copy plugin.xml ..\JSONPlugin\plugins\Import\
copy icon16.png ..\JSONPlugin\plugins\Import\
copy icon32.png ..\JSONPlugin\plugins\Import\
PAUSE
