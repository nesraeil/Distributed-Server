package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.IOException;
import java.io.InputStream;

public interface JavaRunner
{
    /**
     * Reads the source code from the InputStream, compiles it, and runs it. The code must not depend on any classes
     * other than those in the standard JRE. The Java class whose source code is submitted must have a no-args
     * constructor and a no-args method called "start", which is the method that will be invoked to execute the class.
     *
     * See https://www.programcreek.com/java-api-examples/?api=javax.tools.JavaCompiler for related code examples
     *
     * @param in
     * @return the System.err and System.out output from running the compiled Java code
     * @throws IllegalArgumentException
     * @throws IOException
     */
    String compileAndRun(InputStream in) throws IllegalArgumentException, IOException;
}