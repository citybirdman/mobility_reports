# Copyright (c) 2024, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from erpnext.accounts.report.trial_balance_for_party.trial_balance_for_party import execute as trial_balance_for_party_source


def execute(filters=None):
	columns, data = trial_balance_for_party_source(filters)

	modified_data = []
	if filters.get("show_net_values_in_party_account"):
		for row in data:
			if row["debit"] - row["credit"] >= 0:
				row["debit"] = row["debit"] - row["credit"]
			else:
				row["credit"] = row["credit"] - row["debit"]
			modified_data.append(row)
	else:
		modified_data = data

	return columns, modified_data
