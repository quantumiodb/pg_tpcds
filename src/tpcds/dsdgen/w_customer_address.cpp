/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */
#include "w_customer_address.h"

#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"
#include "tpcds_loader.h"

#include <stdio.h>

struct W_CUSTOMER_ADDRESS_TBL g_w_customer_address;

/*
 * mk_customer_address
 */
int mk_w_customer_address(void *info_arr, ds_key_t index) {
  /* begin locals declarations */
  struct W_CUSTOMER_ADDRESS_TBL *r;
  tdef *pTdef = getSimpleTdefsByNumber(CUSTOMER_ADDRESS);

  r = &g_w_customer_address;

  nullSet(&pTdef->kNullBitMap, CA_NULLS);
  r->ca_addr_sk = index;
  mk_bkey(&r->ca_addr_id[0], index, CA_ADDRESS_ID);
  pick_distribution(&r->ca_location_type, "location_type", 1, 1, CA_LOCATION_TYPE);
  mk_address(&r->ca_address, CA_ADDRESS);

  auto &loader = *(tpcds::TableLoader *)info_arr;

  loader.start()
      .addItemKey(r->ca_addr_sk)
      .addItem(r->ca_addr_id)
      .addItem(std::to_string(r->ca_address.street_num).c_str())
      .addItemStreet(r->ca_address)
      .addItem(r->ca_address.street_type)
      .addItem(r->ca_address.suite_num)
      .addItem(r->ca_address.city)
      .addItem(r->ca_address.county)
      .addItem(r->ca_address.state)
      .addItem(([&]{ static char b[8]; snprintf(b,sizeof(b),"%05d",r->ca_address.zip); return b; })())
      .addItem(r->ca_address.country)
      .addItem(std::to_string(r->ca_address.gmt_offset).c_str())
      .addItem(r->ca_location_type)
      .end();

  return 0;
}