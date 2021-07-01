try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import poyonga

setup(
    name="poyonga",
    version=poyonga.__version__,
    description="Python Groonga Client",
    long_description=open("README.rst").read(),
    license="MIT License",
    author="Hideo Hattori",
    author_email="hhatto.jp@gmail.com",
    url="https://github.com/hhatto/poyonga",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    keywords="groonga http gqtp",
    packages=["poyonga"],
    zip_safe=False,
)
