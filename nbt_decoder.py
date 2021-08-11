import base64
import io

import nbt


class NbtDecoder:

  @staticmethod
  def get_item_id_from_bytes(bytes):
    gzipped_nbt = base64.b64decode(bytes)
    gzipped_nbt_stream = io.BytesIO(gzipped_nbt)
    nbtfile = nbt.nbt.NBTFile(fileobj=gzipped_nbt_stream)
    item_data = nbtfile.tags[0].tags[0].tags[2].tags
    for data in item_data:
      if data.name == 'ExtraAttributes':
        for attribute in data.tags:
          if attribute.name == 'id':
            return attribute.value
