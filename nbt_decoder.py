import base64
import io

import nbt


class NbtDecoder:

  @staticmethod
  def get_item_data_from_bytes(bytes):
    result = {}
    gzipped_nbt = base64.b64decode(bytes)
    gzipped_nbt_stream = io.BytesIO(gzipped_nbt)
    nbtfile = nbt.nbt.NBTFile(fileobj=gzipped_nbt_stream)
    item_data = nbtfile.tags[0].tags[0].tags
    result['count'] = item_data[1].value
    for data in item_data[2].tags:
      if data.name == 'ExtraAttributes':
        for attribute in data.tags:
          if attribute.name == 'id':
            result['name'] = attribute.value
            return result
