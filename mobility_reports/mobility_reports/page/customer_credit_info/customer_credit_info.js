const translations = {
	"Ageing Info":"معلومات المتأخرات",
	"Branch":"الفرع",
	"Age":"العمر",
	"Sales Invoices": "فواتير المبيعات",
	"Customer Group":"مجموعة العميل",
	"Customer Info":"معلومات العميل",
	"Customer Name":"اسم العميل",
	"Disabled":"موقوف؟",
	"Grand Total":"المبلغ الإجمالي",
	"Internal Credit Limit":"الحد الإئتماني الداخلي",
	"Legal Credit Limit":"الحد الإئتماني القانوني",
	"Outstanding Amount":"المبلغ المستحق",
	"Outstanding":"الرصيد المستحق",
	"Payment Entries":"قيود السداد",
	"Payment Terms":"شروط الدفع",
	"Posting Date":"التاريخ",
	"Received Amount":"المبلغ المستلم",
	"Sales Invoice":"فاتورة المبيعات",
	"Sales Invoices":"فواتير المبيعات",
	"Sales Person":"مندوب المبيعات",
	"Unbilled Notes":"أذون غير مفوترة",
	"Customer":"العميل"};
frappe.pages['customer-credit-info'].on_page_load = function(wrapper) {
	let page = frappe.ui.make_app_page({
		parent: wrapper,
		title: 'Customer Credit Information',
		single_column: true
	});

	// Set base layout
	$(page.body).html(`
		<div class="p-4">
			<div class="row filters mb-4">
				<div class="col-md-4" id="company_control_wrapper"></div>
				<div class="col-md-4" id="customer_control_wrapper"></div>
				<div class="col-md-4 d-flex align-items-end">
					<button class="btn btn-primary" id="load_data">Load Data</button>
				</div>
			</div>
			<div id="result_tables"></div>
		</div>
	`);

	// Create company control
    let company_control = frappe.ui.form.make_control({
        parent: $('#company_control_wrapper')[0],
        df: {
            label: "Company",
            fieldname: "company",
            fieldtype: "Link",
            options: "Company",
            placeholder: "Select Company",
            reqd: 1
        },
        render_input: true
    });
    company_control.set_value(frappe.defaults.get_global_defaults("company")[0]);

	// Create customer control
	let customer_control = frappe.ui.form.make_control({
		parent: $('#customer_control_wrapper')[0],
		df: {
			label: "Customer",
			fieldname: "customer",
			fieldtype: "Link",
			options: "Customer",
			placeholder: "Select Customer",
			reqd: 1
		},
		render_input: true
	});

	// On button click
	$('#load_data').on('click', function () {
		let company = company_control.get_value();
		let customer = customer_control.get_value();

		if (!company || !customer) {
			frappe.msgprint("Please select both Company and Customer.");
			return;
		}

		frappe.call({
			method: "mobility_reports.customer_info.data_execute",
			args: {
				company: company,
				customer: customer
			},
			callback: function (r) {
				if (r.message) {
					renderTables(r.message);
				} else {
					frappe.msgprint("No data returned.");
				}
			}
		});
	});

	// Table rendering
	function renderTables(data) {
		let html = `
			<div class="mb-4">${buildTable("Customer Info", data.customer)}</div>
            <div class="mb-4">${buildTable("Ageing Info", data.ageing)}</div>
			<div class="row">
                <div class="col-md-9">${buildTable("Sales Invoices", data.sales_invoice)}</div>
                <div class="col-md-3">${buildTable("Payment Entries", data.payment_entry)}</div>
			</div>
		`;
		if (frappe.boot.lang === "ar") {
			console.log("good")
			Object.keys(translations).forEach(english => {
				const arabic = translations[english];
				html = html.replaceAll(english, arabic);
			});
		}
		$('#result_tables').html(html);
	}

	// Helper to format table headers
	function formatHeader(str) {
		return str.replace(/_/g, " ")
				  .split(" ")
				  .map(word => word.charAt(0).toUpperCase() + word.slice(1))
				  .join(" ");
	}

	// Table builder
	function buildTable(title, data) {
		if (!data || data.length === 0) {
			return `<div class="text-muted">${title}: No data available.</div>`;
		}

		let keys = Object.keys(data[0]);
		let headers = keys.map(k => `<th>${formatHeader(k)}</th>`).join("");

		let rows = data.map(row => {
			let tds = keys.map(k => `<td>${frappe.utils.escape_html(row[k] || '')}</td>`).join("");
			return `<tr>${tds}</tr>`;
		}).join("");

		return `
			<div class="frappe-card mb-4">
				<div class="frappe-card-head"><strong>${title}</strong></div>
				<div class="frappe-card-body">
					<div class="table-responsive">
						<table class="table table-bordered table-hover">
							<thead class="thead-light"><tr>${headers}</tr></thead>
							<tbody>${rows}</tbody>
						</table>
					</div>
				</div>
			</div>
		`;
	}
};
