package de.oswoboda.cleanup;

import org.apache.accumulo.core.util.CleanUp;

/**
 * Hello world!
 *
 */
public class Main 
{
    public static void main( String[] args )
    {
        CleanUp.shutdownNow();
    }
}
