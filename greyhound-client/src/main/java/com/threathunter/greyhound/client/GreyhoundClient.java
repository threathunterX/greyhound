package com.threathunter.greyhound.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by daisy on 17/6/26.
 */
public class GreyhoundClient {
    public void readEventsFromFile(String file) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

    }
}
