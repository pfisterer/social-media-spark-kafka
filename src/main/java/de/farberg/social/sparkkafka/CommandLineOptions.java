package de.farberg.social.sparkkafka;

import java.beans.Transient;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class CommandLineOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	@Option(name = "--zookeeper", usage = "Zookeeper bootstrap server", required = true)
	public String zookeeperServer = null;
	
	@Option(name = "--kafka-bootstrap-server", usage = "Kafka bootstrap server", required = true)
	public String kafkaBootstrapServer = null;

	@Option(name = "--kafka-source-topic", usage = "Kafka topic to read data from", required = true)
	public String kafkaSourceTopic = null;

	@Option(name = "--kafka-dest-topic", usage = "Kafka topic to send data to", required = true)
	public String kafkaDestinationTopic = null;

	@Option(name = "--kafka-group-id", usage = "Kafka group to use", required = false)
	public String kafkaGroupId = "Bla";

	@Option(name = "--verbose", usage = "Verbose (DEBUG) logging output (default: INFO).", required = false)
	public boolean verbose = false;

	@Option(name = "-h", aliases = { "--help" }, usage = "This help message.", required = false)
	public boolean help = false;

	@Transient
	static CommandLineOptions parseCmdLineOptions(final String[] args) {
		CommandLineOptions options = new CommandLineOptions();
		CmdLineParser parser = new CmdLineParser(options);

		try {
			parser.parseArgument(args);
			if (options.help)
				printHelpAndExit(parser);

			if (options.verbose) {
				org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
			}

		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			printHelpAndExit(parser);
		}

		return options;
	}

	@Transient
	private static void printHelpAndExit(CmdLineParser parser) {
		System.err.print("Usage: java " + Main.class.getCanonicalName());
		parser.printSingleLineUsage(System.err);
		System.err.println();
		parser.printUsage(System.err);
		System.exit(1);
	}
}