Native build recipes use GCC runtime libraries on some platforms.
These runtimes are covered by their upstream GNU license with the GCC Runtime Library Exception 3.1.
This does not select GPL for the Java/JNI bindings or for Zstandard.
The fixed exception text is supplied from the GCC 15.2.0 source mirror; this is not a claim that every native binary was compiled with that GCC version.
