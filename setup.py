from setuptools import setup

setup(name='publish_naduf',
      version='0.1',
      description='Upload of NADUF data to ERIC/internal',
      url='https://github.com/eawag-rdm/publish_naduf',
      author='Harald von Waldow',
      author_email='harald.vonwaldow@eawag.ch',
      license='GNU Affero General Public License',
      packages=['upload_naduf'],
      zip_safe=False,
      install_requires=['ckanapi'],
      entry_points={
          'console_scripts': ['upload_naduf=upload_naduf.upload_naduf:main']
      }
)
