from setuptools import setup

setup(
    name="spice-switch",
    version="0.1",
    py_modules=["spice_switch"],
    install_requires=[
        "prompt_toolkit",
    ],
    entry_points={
        "console_scripts": [
            "spice-switch=spice_switch:main",
        ],
    },
)
