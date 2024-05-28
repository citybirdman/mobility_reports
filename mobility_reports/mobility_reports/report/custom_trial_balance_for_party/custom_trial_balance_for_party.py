# Copyright (c) 2024, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from erpnext.accounts.report.trial_balance_for_party.trial_balance_for_party import execute as trial_balance_for_party_source
from erpnext.accounts.report.trial_balance_for_party.trial_balance_for_party import toggle_debit_credit



def execute(filters=None):
	company_currency = frappe.get_cached_value("Company", filters.company, "default_currency")
	columns, data = trial_balance_for_party_source(filters)
	total_row = frappe._dict(
		{
			"opening_debit": 0,
			"opening_credit": 0,
			"debit": 0,
			"credit": 0,
			"closing_debit": 0,
			"closing_credit": 0,
		}
	)
	modified_data = []
	if filters.get("show_net_values_in_party_account"):
		data.pop()
		for row in data:
			if row["debit"] - row["credit"] >= 0:
				row["debit"] = row["debit"] - row["credit"]
				row["credit"] = 0
			else:
				row["credit"] = row["credit"] - row["debit"]
				row["debit"] = 0

			closing_debit, closing_credit = toggle_debit_credit(
				row["opening_debit"] + row["debit"], row["opening_credit"] + row["credit"]
			)
			row["closing_debit"] = closing_debit
			row["closing_credit"] = closing_credit

			modified_data.append(row)
			for col in total_row:
				total_row[col] += row.get(col)

		total_row.update({"party": "'" + _("Totals") + "'", "currency": company_currency})
		modified_data.append(total_row)
	else:
		modified_data = data

	return columns, modified_data
