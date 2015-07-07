# -*- mode: python -*-
a = Analysis(['ImportJSON.py'],
             pathex=None,
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None,
             excludes=['_tkinter', 'IPython', 'pandas', 'numpy'])

pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='ImportJSON.exe',
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
               name='ImportJSON')
