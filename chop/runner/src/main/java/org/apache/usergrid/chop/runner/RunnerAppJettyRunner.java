/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.chop.runner;


import org.safehaus.jettyjam.utils.ContextListener;
import org.safehaus.jettyjam.utils.FilterMapping;
import org.safehaus.jettyjam.utils.HttpsConnector;
import org.safehaus.jettyjam.utils.JettyConnectors;
import org.safehaus.jettyjam.utils.JettyContext;
import org.safehaus.jettyjam.utils.JettyRunner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.google.inject.servlet.GuiceFilter;


/**
 * Launches the Runner web application and has the main() entry point for the executable
 * jar file for the Runner.
 */
@JettyContext(
    enableSession = true,
    contextListeners = { @ContextListener( listener = RunnerConfig.class ) },
    filterMappings = { @FilterMapping( filter = GuiceFilter.class, spec = "/*") }
)
@JettyConnectors(
    defaultId = "https",
    httpsConnectors = { @HttpsConnector( id = "https", port = 0 ) }
)
public class RunnerAppJettyRunner extends JettyRunner {
    private static RunnerAppJettyRunner instance;

    private static CommandLine cl;


    protected RunnerAppJettyRunner() {
        super( RunnerAppJettyRunner.class.getSimpleName() );
        instance = this;
    }


    public static RunnerAppJettyRunner getInstance() {
        return instance;
    }

    @Override
    public String getSubClass() {
        return getClass().getName();
    }



    public static void main( String[] args ) throws Exception {
        processCli(args);
        RunnerAppJettyRunner launcher = new RunnerAppJettyRunner();
        launcher.start();
    }


    public static CommandLine getCommandLine() {
        return cl;
    }


    static void processCli(String[] args) {
        CommandLineParser parser = new PosixParser();
        Options options = getOptions();

        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            if (e instanceof MissingArgumentException) {
                System.out.println("Missing option: " + ((MissingArgumentException) e).getOption());
            }

            help(options);
            System.exit(1);
        }

        if (cl.hasOption('h')) {
            help(options);
            System.exit(0);
        }
    }


    static void help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ChopRunner", options);
    }

    static Options getOptions() {
        Options options = new Options();

        options.addOption( "h", "help", false, "Print out help." );
        options.addOption( "p", "service-provider", true, "Overrides the service provider(AWS by default)" );

        return options;
    }
}
