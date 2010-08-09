from distutils.core import setup

version = '1.0rc1'

setup(name='txcron',
      version=version,
      description="Schedule at-style, cron-style or interval jobs in Twisted",
      author='Jason Beck',
      author_email='jsbeck@subfx.net',
      url='http://subfx.net',
      license='MIT',
      packages=['txcron',],
      package_dir={'txcron':'src/txcron',})
