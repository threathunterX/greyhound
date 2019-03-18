package com.threathunter.greyhound.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * 
 */
public class GreyhoundClient {
    public void readEventsFromFile(String file) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

    }
}
