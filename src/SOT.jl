module SOT

using XLSX, DataFrames, CSV, LORISQL, Statistics

const CODING = DataFrame(
	SOT=[1, 2, 3, 4, 5, 6],
	Visual=[2, 0, 1, 2, 0, 1],
	Prop=[1, 1, 1, 0, 0, 0]
)

function trimSOT!(sot::DataFrame;cols::Vector{Symbol}=[
		:SOT,
		:ID,
		:speedCOP,
		:rmsCOP,
		:freqCOP
	])::Nothing
	select!(sot, cols)
	return nothing
end

function readSOT(path::String;dotrim::Bool=true)::DataFrame
	# Reading Excel file
	xlfile = XLSX.readxlsx(path)
	# Initializing SOT names
	sotnames = ["SOT$n" for n in 1:6]
	# Extracting SOT matrices from Excel
	sotmats = [xlfile[sot][:] for sot in sotnames]
	# Replacing periods with missing values
	replace!.(sotmats, "." => missing)
	# Getting column names
	colnames = string.(sotmats[1][1,:])
	# Getting column data
	sotdata = [sot[2:end,:] for sot in sotmats]
	# Combining column names and data into dataframes
	sotdfs = map(sot -> DataFrame(sot, colnames), sotdata)
	# Fixing names and creating SOT column
	foreach(enumerate(sotdfs)) do (id,df)
		dfcols = names(df)
		newdfcols = Symbol.(first.(split.(dfcols, '.')))
		rename!(df, newdfcols)
		df[!,"SOT"] .= id
	end
	# Concatenating all the dataframe into one
	output = vcat(sotdfs...)
	dotrim && trimSOT!(output)
	return output
end

function getSOT(sot::DataFrame, n::Int64)::DataFrame
	return filter(row -> row.SOT == n, sot)
end

function getPts(sot::DataFrame)::DataFrame
	# Getting relevant patient ids
	pts = sot.ID
	# Pulling study participants
	ppg_pts = LORISQL.studypatients("PPG")
	# Renaming weird column
	rename!(ppg_pts, :visit_id_list => :ID)
	# Fixing ID column
	ppg_pts[!,:ID] = map(ppg_pts.ID) do idlist
		ids = idlist |> split .|> string
		filter!(id -> occursin("PPG", id), ids)
		return first(ids)
	end
	# Filtering all but baseline rows, and only relevant pts
	filter!(ppg_pts) do row
		occursin("ppg_baseline", row.visit_types_list) &&
		row.ID in pts
	end
	# Final cleanups
	sort!(ppg_pts, :ID)
	select!(ppg_pts, [:ID, :candid, :sessionid])
	return ppg_pts
end

function joinTable(pts::DataFrame, what::String;cols=[])::DataFrame
	newtable = LORISQL.selectall(what)
	filter!(row -> !ismissing(row.sessionid), newtable)
	!isempty(cols) && select!(newtable, [:candid, :sessionid, cols...])
	output = leftjoin(pts, newtable, on=[:candid, :sessionid])
	return output
end

function joinTable(dest::DataFrame, src::DataFrame)::DataFrame
	select!(src, Not([:sessionid, :candid]))
	newtable = leftjoin(dest, src, on=:ID)
	return newtable
end

function getRawSOT(path::String)::DataFrame
	sotData = readSOT(path)
	sotPts = getPts(sotData)
	motorTable = joinTable(sotPts, "R_motor";cols=[:hy])
	sotData = joinTable(sotData, motorTable)
	sotData[!,:hy] = parse.(Float64, sotData.hy)
	sotData[!,:group] = map(id -> id[1:end-3], string.(last.(split.(sotData.ID, '-'))))
	return sotData
end

function getStatsCOP(data::GroupedDataFrame)::DataFrame
	return combine(
		data,
		[:rmsCOP, :speedCOP, :freqCOP] => ( (rms, speed, freq) -> begin
			nomissing = vec -> filter(!ismissing, vec)
			rms_nona = nomissing(rms)
			speed_nona = nomissing(speed)
			freq_nona = nomissing(freq)
			(
				rms_mean=mean(rms_nona),
				rms_std=std(rms_nona),
				speed_mean=mean(speed_nona),
				speed_std=std(rms_nona),
				freq_mean=mean(freq_nona),
				freq_std=std(freq_nona)
			)
		end) => AsTable
	)
end

end # module
