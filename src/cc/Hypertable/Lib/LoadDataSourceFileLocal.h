/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_LOADDATASOURCEFILELOCAL_H
#define HYPERTABLE_LOADDATASOURCEFILELOCAL_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/String.h"

#include "DataSource.h"
#include "FixedRandomStringGenerator.h"
#include "LoadDataSource.h"


namespace Hypertable {

  class LoadDataSourceFileLocal : public LoadDataSource {
  private:
    uint32_t file_size;
    uint32_t num_parallel;
    uint32_t curr_cursor;
    std::vector<uint32_t> init_cursors;
    std::vector<uint32_t> cursors;

  public:
    LoadDataSourceFileLocal(const String &fname, const String &header_fname,
                            int row_uniquify_chars = 0,
                            bool dupkeycol = false,
			    int parallel = 1);

    ~LoadDataSourceFileLocal();

    bool next(uint32_t *type_flagp, KeySpec *keyp,
	      uint8_t **valuep, uint32_t *value_lenp,
	      uint32_t *consumedp, std::string &consumed_line);

    uint64_t incr_consumed();

  protected:
    String get_header();
    void init_src();

    boost::iostreams::file_source m_source;
    String m_header_fname;
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCEFILELOCAL_H
