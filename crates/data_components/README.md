# Spice.ai Data Components

This crate implements DataFusion TableProviders for reading, writing and streaming Arrow data to/from various systems.

Each component has up to 2 capabilities:
- **Read**: **Required** Read data from the component, implemented via TableProvider.scan().
- **Write**: **Optional** Write data to the component, implemented via TableProvider.insert_into().
