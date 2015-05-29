# -*- mode: python -*-
a = Analysis(['ExportJSON.py'],
             pathex=None,
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None,
             excludes=['_tkinter', 'IPython'])
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='ExportJSON.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=None,
               upx=True,
               name='ExportJSON')
