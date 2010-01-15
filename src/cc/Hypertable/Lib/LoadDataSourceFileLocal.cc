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

#include "Common/Compat.h"
#include <fstream>
#include <iostream>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/FileUtils.h"

#include "Key.h"
#include "LoadDataSourceFileLocal.h"

using namespace boost::iostreams;
using namespace Hypertable;
using namespace std;

/**
 *
 */
LoadDataSourceFileLocal::LoadDataSourceFileLocal(const String &fname,
						 const String &header_fname, int row_uniquify_chars, bool dupkeycols, int parallel)
  : LoadDataSource(row_uniquify_chars, dupkeycols),
    m_source(fname), m_header_fname(header_fname) {

  if (!FileUtils::exists(fname.c_str()))
    HT_THROW(Error::FILE_NOT_FOUND, fname);

  if (boost::algorithm::ends_with(fname, ".gz")) { 
    m_fin.push(gzip_decompressor());
    m_zipped = true;

    if(parallel > 1) 
      HT_THROW(Error::HQL_PARSE_ERROR, "LOAD DATA - parallel option not supported for compressed files ");
  }

  num_parallel = parallel;
  curr_cursor = 0;

  struct stat filestatus;

  if(stat(fname.c_str(), &filestatus) == 0)
    file_size = filestatus.st_size;
  else {
    cout << "Error while obtaining file size " << endl;
    HT_THROW(Error::FILE_NOT_FOUND, "Error while obtaining file size");
  }

  for(uint32_t i = 0; i < num_parallel; ++i) {
    cursors.push_back(0);
    init_cursors.push_back(0);
  }

  return;
}

/**
 *
 */
LoadDataSourceFileLocal::~LoadDataSourceFileLocal() {
  printf("In load data file local: destructor \n");

}

void
LoadDataSourceFileLocal::init_src()
{
  m_fin.push(m_source);

  // initialize cursors
  uint32_t i = 0;

  cursors[0] = 0;
  init_cursors[0] = 0;

  cout << "Setting cursor for " << 0 << " - " << cursors[0] << endl;

  for(i = 1; i < num_parallel; ++i) {
    uint32_t tmp_cursor = (uint32_t) (i * file_size / num_parallel);

    if(tmp_cursor < cursors[i - 1])
      tmp_cursor = cursors[i - 1];

    cursors[i] = tmp_cursor;
    init_cursors[i] = tmp_cursor;

    cout << "Init cursor for " << i << " - " << tmp_cursor << endl;

    // get the cursors around the new line
    m_fin.seekg(tmp_cursor);

    String tmp_line;

    while(getline(m_fin, tmp_line)) {
      cursors[i] = m_fin.tellg();
      init_cursors[i] = m_fin.tellg();
      break;
    }

    cout << "Setting cursor for " << i << " - " << cursors[i] << endl;
  }

  // reset seek
  m_fin.seekg(0);
}

String
LoadDataSourceFileLocal::get_header()
{
  String header = "";
  if (m_header_fname != "") {
    std::ifstream in(m_header_fname.c_str());
    getline(in, header);
  }
  else {
    getline(m_fin, header);

    // increment cursor 0
    cursors[0] += header.length() + 1;
  }

  return header;
}

bool 
LoadDataSourceFileLocal::next(uint32_t *type_flagp, KeySpec *keyp,
			      uint8_t **valuep, uint32_t *value_lenp,
			      uint32_t *consumedp, std::string &consumed_line) {
  consumed_line = "";

  // set the seek
  uint32_t tmp_num  = 0;

  bool ret = false;

  cout << "In next .. " << endl;

  while(tmp_num++ < num_parallel) {
    // clear error flags
    m_fin.clear();

    cout << "Cursor: " << curr_cursor << " index: "  << cursors[curr_cursor] << endl;

    // check if this segment is done
    if(curr_cursor < (num_parallel - 1) &&
       cursors[curr_cursor] == init_cursors[curr_cursor + 1]) {
      cout << "Curr segment done for cursor: " << curr_cursor << "Current index: " << cursors[curr_cursor] << endl;
      // increment the cursor
      curr_cursor = (curr_cursor + 1) % num_parallel;
      continue;
    }

    m_fin.seekg(cursors[curr_cursor]);

    ret = LoadDataSource::next(type_flagp, keyp,
			       valuep, value_lenp, consumedp, consumed_line);

    cout << "Input line has been consumed: " << consumed_line << endl;
    cursors[curr_cursor] += consumed_line.length() + 1;
    // increment the cursor index and cursor
    curr_cursor = (curr_cursor + 1) % num_parallel;

    if(ret == true)
      break;
  }

  return ret;
}

/**
 *
 */
uint64_t
LoadDataSourceFileLocal::incr_consumed()
{
  //  cout << "In incr consumed " << endl;
  uint64_t consumed=0;
  uint64_t new_offset = m_source.seek(0, BOOST_IOS::cur);
  consumed = new_offset - m_offset;
  m_offset = new_offset;
  return consumed;
}

