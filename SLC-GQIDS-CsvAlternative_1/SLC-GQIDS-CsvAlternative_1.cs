using Skyline.DataMiner.Analytics.GenericInterface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

[GQIMetaData(Name = "Csv file alternative")]
public class CsvAlternativeDataSource : IGQIDataSource, IGQIInputArguments
{
    private const string CSV_ROOT_PATH = @"C:\Skyline DataMiner\Documents";
    private const string FILE_ARGUMENT_NAME = "File";

    private readonly GQIStringArgument _delimiterArgument;

    private string _csvFilePath;
    private string _delimiter;

    public CsvAlternativeDataSource()
    {
        _delimiterArgument = new GQIStringArgument("Delimiter")
        {
            DefaultValue = ",",
        };
    }

    private static string[] GetCsvFileOptions()
    {
        if (!Directory.Exists(CSV_ROOT_PATH))
            throw new GenIfException($"Csv file root path does not exist: {CSV_ROOT_PATH}");

        return Directory.EnumerateFiles(CSV_ROOT_PATH, "*.csv", SearchOption.AllDirectories)
            .Select(fileName =>
            {
                var relativeFileName = fileName.Substring(CSV_ROOT_PATH.Length + 1, fileName.Length - CSV_ROOT_PATH.Length - 5);
                return relativeFileName.Replace(@"\", "/");
            })
            .ToArray();
    }

    public GQIArgument[] GetInputArguments()
    {
        var csvFileOptions = GetCsvFileOptions();
        if (csvFileOptions.Length == 0)
            throw new GenIfException($"No csv files available in '{CSV_ROOT_PATH}'.");

        var fileArgument = new GQIStringDropdownArgument(FILE_ARGUMENT_NAME, csvFileOptions)
        {
            IsRequired = true,
        };

        return new GQIArgument[]
        {
            fileArgument,
            _delimiterArgument,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        var fileArgument = new GQIStringArgument(FILE_ARGUMENT_NAME);
        var csvFileOption = args.GetArgumentValue(fileArgument);

        if (string.IsNullOrEmpty(csvFileOption))
            throw new GenIfException("Missing csv file.");

        var relativeFileName = csvFileOption.Replace("/", @"\");
        _csvFilePath = $@"{CSV_ROOT_PATH}\{relativeFileName}.csv";

        if (!File.Exists(_csvFilePath))
            throw new GenIfException($"Csv file does not exist: {_csvFilePath}");

        _delimiter = args.GetArgumentValue(_delimiterArgument);
        if (string.IsNullOrEmpty(_delimiter))
            _delimiter = ",";

        return default;
    }

    public GQIColumn[] GetColumns()
    {
        return ReadColumns();
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        var rows = ReadRows();
        return new GQIPage(rows);
    }

    private GQIColumn[] ReadColumns()
    {
        using (var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        using (var streamReader = new StreamReader(fileStream))
        {
            var header = streamReader.ReadLine();
            return CreateColumns(header);
        }
    }

    private GQIRow[] ReadRows()
    {
        var rows = new List<GQIRow>();

        using (var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        using (var streamReader = new StreamReader(fileStream))
        {
            // Skip header
            streamReader.ReadLine();

            while (!streamReader.EndOfStream)
            {
                var line = streamReader.ReadLine();
                var row = CreateRow(line);
                rows.Add(row);
            }
        }

        return rows.ToArray();
    }

    private GQIColumn<string>[] CreateColumns(string header)
    {
        var names = header.Split(new[] { _delimiter }, StringSplitOptions.None);

        return names
            .Select(name => new GQIStringColumn(name.Trim('"')))
            .ToArray();
    }

    private GQIRow CreateRow(string line)
    {
        var values = line.Split(new[] { _delimiter }, StringSplitOptions.None);

        var cells = values
            .Select(value => new GQICell { Value = value.Trim('"') })
            .ToArray();

        return new GQIRow(cells);
    }
}