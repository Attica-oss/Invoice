// Converts the Container Operations Activity data into a report for the STOs.
// Limitation: Need to create the logic where containers are completed by Cross Stuffing operations.
// To add the Remarks " Haulage in STO [Vessel] on [Date] @ [Time]"
let
    // Parameters
    vesselName = let
    VesselName =
        Text.Upper(
            Text.Trim(
                Excel.CurrentWorkbook(){[Name="Vessel_Name"]}[Content]{0}[Column1]
            )
        )
in
    VesselName,
    // Start Date
    startDate =
        let
            StartDate = Date.From(Excel.CurrentWorkbook(){[Name = "Start_Date"]}[Content]{0}[Column1])
        in
            StartDate,
    // End Date
    endDate = let EndDate = Date.From(Excel.CurrentWorkbook(){[Name = "End_Date"]}[Content]{0}[Column1]) in EndDate,
    // Source
    dropboxFilePath = Excel.CurrentWorkbook(){[Name = "Dropbox_Path"]}[Content]{1}[Path],
    rawCsv = Csv.Document(File.Contents(dropboxFilePath)),
    promotedHeaders = Table.PromoteHeaders(rawCsv, [PromoteAllScalars = true]),
    bufferedTable = Table.Buffer(promotedHeaders),
    // Standardize column names
    renamedCols = Table.RenameColumns(
        bufferedTable,
        {
            {"vessel_client", "Vessel"},
            {"date_plugged", "Date affected"},
            {"container_number", "Container Ref. No."},
            {"customer", "Customer"},
            {"time_plugged", "Plugin Time"},
            {"shipping_line", "Shipping Line"},
            {"plugged_status", "Status"},
            {"tonnage", "Tonnage"},
            {"set_point", "SP"},
            {"date_out", "Unplugged Date"}
        }
    ),
    typedCols = Table.TransformColumnTypes(
        renamedCols,
        {
            {"Date affected", type date},
            {"Plugin Time", type time},
            {"Tonnage", type number},
            {"SP", Int64.Type},
            {"Unplugged Date", type date}
        }
    ),
    // Filters (expects vesselName, startDate, endDate to exist as parameters)
    filteredRows = Table.SelectRows(
        typedCols,
        each Text.Contains([Vessel], vesselName) and ([Date affected] >= startDate and [Date affected] <= endDate)
    ),
    // Merge date + time into datetime
    mergedDateTime = Table.CombineColumns(
        filteredRows,
        {"Date affected", "Plugin Time"},
        (cols) => List.First(cols) & List.Last(cols),
        "Date Time Plugged in"
    ),
    typedDateTime = Table.TransformColumnTypes(mergedDateTime, {{"Date Time Plugged in", type datetime}}),
    // Keep needed columns for calculations
    selectedCols = Table.SelectColumns(
        typedDateTime,
        {
            "Vessel",
            "Customer",
            "Date Time Plugged in",
            "Container Ref. No.",
            "operation_type",
            "Shipping Line",
            "Status",
            "Tonnage",
            "SP",
            "Unplugged Date",
            "location"
        }
    ),
    // Days on Plug
    withDaysOnPlug = Table.AddColumn(
        selectedCols,
        "Days on Plug",
        each
            let
                plugStatus = [Status],
                oneDay = #duration(1, 0, 0, 0),
                plugInDate = Date.From([Date Time Plugged in]),
                plugOutDate = [Unplugged Date],
                baseDuration = plugOutDate - plugInDate,
                adjustedDuration = if plugStatus = "Partial" then baseDuration else baseDuration + oneDay
            in
                Duration.Days(adjustedDuration),
        type number
    ),
    // Invoice Value
    withInvoiceValue = Table.AddColumn(
        withDaysOnPlug,
        "Invoice Value ($)",
        each
            let
                // Monitoring
                monitoringRate = 30,
                isPartialCompletion = [location] = "For Completion",
                monitoringCharge = if isPartialCompletion then 0 else monitoringRate,
                // Plugin
                pluginRate = 25,
                isExchangeHands = [location] = "Exchange of Hands",
                pluginCharge = if isExchangeHands then 0 else pluginRate,
                // Electricity
                standardRate = 70,
                magnumRate = 100,
                superFreezerRate = 110,
                sp = [SP],
                electricityRate =
                    if sp = -25 then
                        standardRate
                    else if sp = -35 then
                        magnumRate
                    else if sp = -60 then
                        superFreezerRate
                    else
                        null,
                electricityCharge = if electricityRate = null then null else electricityRate * [Days on Plug],
                totalCharge = List.Sum({electricityCharge, monitoringCharge, pluginCharge}),
                // Non-billable customers
                nonBillableCustomers = {"MAERSKLINE", "IOT"},
                finalCharge = if List.Contains(nonBillableCustomers, [Customer]) then 0 else totalCharge
            in
                finalCharge,
        Currency.Type
    ),
    // Remarks
    withRemarks = Table.AddColumn(
        withInvoiceValue,
        "Remarks",
        each
            let
                maerskCustomer = "MAERSKLINE",
                iotCustomer = "IOT",
                normalLocations = {"LML", "IOT", "For Completion", "Fishing Port"},
                locText = if List.Contains(normalLocations, [location]) then "" else [location],
                remarkText =
                    if [Customer] = maerskCustomer then
                        "On the account of MAERSK"
                    else if [Customer] = iotCustomer then
                        "On the account of " & iotCustomer
                    else
                        locText
            in
                remarkText
    ),
    // ✅ Force Days on Plug = 0 when Remarks indicate Maersk or IOT account
    adjustedDaysOnPlug = Table.AddColumn(
        withRemarks,
        "Days on Plug (Adj)",
        each
            if
                Text.Contains([Remarks], "On the account of MAERSK")
                or Text.Contains([Remarks], "On the account of IOT")
            then
                0
            else
                [Days on Plug],
        type number
    ),
    // Replace original Days on Plug with adjusted one
    removedOldDays = Table.RemoveColumns(adjustedDaysOnPlug, {"Days on Plug"}),
    renamedAdjustedDays = Table.RenameColumns(removedOldDays, {{"Days on Plug (Adj)", "Days on Plug"}}),
    // Keep final columns
    finalCols = Table.SelectColumns(
        renamedAdjustedDays,
        {
            "Vessel",
            "Date Time Plugged in",
            "Container Ref. No.",
            "Shipping Line",
            "Status",
            "Tonnage",
            "SP",
            "Unplugged Date",
            "Days on Plug",
            "Invoice Value ($)",
            "Remarks"
        }
    ),
    reordered = Table.ReorderColumns(
        finalCols,
        {
            "Vessel",
            "Date Time Plugged in",
            "Container Ref. No.",
            "Shipping Line",
            "Status",
            "SP",
            "Tonnage",
            "Unplugged Date",
            "Remarks"
        }
    )
in
    reordered
