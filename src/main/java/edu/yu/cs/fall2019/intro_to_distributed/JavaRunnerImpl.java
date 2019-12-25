package edu.yu.cs.fall2019.intro_to_distributed;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaRunnerImpl implements JavaRunner{

    Path outPath;

    public JavaRunnerImpl() {
        try {
            outPath = Files.createTempDirectory("tmp");
        } catch (Exception e) {}
    }


    public JavaRunnerImpl(Path sourceCodePath) {
        outPath = sourceCodePath;
    }

    public String compileAndRun(InputStream is) throws IllegalArgumentException, IOException {
        String output;
        try{
            String code = new String(Util.readAllBytes(is));
            File source = createSource(code);
            compile(source);
            output = run(code);
        } catch (Exception e) {
            output = "400 error: " + e.getMessage();
        }
        return output;
    }

    private String run(String code) throws IOException {
        String output = "";

        synchronized (JavaRunner.class) {
            ClassLoader loader = new URLClassLoader(new URL[]{outPath.toUri().toURL()});
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();

            PrintStream oldOut = System.out;
            PrintStream oldErr = System.out;

            try {
                String pckg = getPackage(code);
                Class<?> cl;
                if(!pckg.equals("")) {
                    cl = loader.loadClass(getPackage(code) + '.' + getClass(code));
                } else {
                    cl = loader.loadClass(getClass(code));
                }
                Object obj = cl.newInstance();


                System.setOut(new PrintStream(out));
                System.setErr(new PrintStream(err));

                cl.getMethod("run").invoke(obj);
                //output = "System.err:\n" + err + "\nSystem.out:\n" + out;

            } catch (InvocationTargetException|IllegalArgumentException e) {
                System.err.println(e.getCause().toString() + " on line "  +
                        e.getCause().getStackTrace()[0].getLineNumber() + " in " +
                        e.getCause().getStackTrace()[0].getClassName() + ".");
            } catch (NoSuchMethodException e) {//CONFIRM THAT THIS SHOULD BE HERE
                System.err.println("Source code does not contain run() method.");
                //throw new IllegalArgumentException("Source code does not contain run() method");
            } catch (Exception e) {
                System.err.println("Unable to run source code.");
                //e.printStackTrace();
            } finally {
                output = "System.err:\n" + err + "\nSystem.out:\n" + out;
                //output += code;
                System.out.flush();
                System.err.flush();

                System.setOut(oldOut);
                System.setErr(oldErr);
            }

        }
        return output;
    }

    /**
     * Pretty much copied straight from https://docs.oracle.com/javase/8/docs/api/javax/tools/JavaCompiler.html
     * @param source
     * @throws IOException
     */
    private void compile(File source) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(source));
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits);
        compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits).call();

        for ( Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            String error = String.format("Error on line %d, column %d in %s%n",
                    diagnostic.getLineNumber(),
                    diagnostic.getColumnNumber(),
                    diagnostic.getSource().toUri());
            throw new IllegalArgumentException(error);
        }
        fileManager.close();
    }

    private File createSource(String code) throws IOException{
        String packageName = getPackage(code);
        packageName = packageName.replace('.', '\\');
        String className = getClass(code);
        String source = outPath.toString() + '\\' + packageName + '\\' + className + ".java";

        File file = new File(source);
        file.getParentFile().mkdirs();
        file.createNewFile();

        FileWriter writer = new FileWriter(source, false);
        writer.write(code);
        writer.close();

        return file;
    }

    /*
    private String streamToString(InputStream in) throws IOException {
        StringBuilder result = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String currentLine = reader.readLine();
            while (currentLine != null) {
                result.append(currentLine + '\n');
                currentLine = reader.readLine();
            }
        }
        return result.toString();
    }
*/
    private String getPackage(String input) {
        Pattern p = Pattern.compile("package\\s+([\\w\\.]+);");
        Matcher m = p.matcher(input);
        if(m.find()) {
            return m.group(1);
        } else {
            return "";
        }
    }

    private String getClass(String input) {
        Pattern p = Pattern.compile("(?<=\\n|\\A)(?:public\\s)?(class|interface|enum)\\s([^\\n\\s]*)");
        Matcher m = p.matcher(input);
        if(m.find()) {
            return m.group(2);
        } else {
            return "";
        }
    }

    /*public static void main(String[] args) throws IOException {
        JavaRunner test = new JavaRunnerImpl();
        String code = "package edu.yu.cs.fall2019.intro_to_distributed.stage1;\n" +
                "\n" +
                "public class HelloWorld {\n" +
                "\tpublic void run() {\n" +
                "\t\tSystem.out.print(\"Hello System.out world!\\n\");\n" +
                "\t\tSystem.err.print(\"Hello System.err world!\\n\");\n" +
                "\t}\n" +
                "}";
        InputStream is = new ByteArrayInputStream(code.getBytes());
        System.out.println(test.compileAndRun(is));
    }*/
}
