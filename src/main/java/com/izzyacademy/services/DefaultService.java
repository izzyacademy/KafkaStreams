package com.izzyacademy.services;

import java.awt.*;

public class DefaultService implements StreamMicroService {

    @Override
    public void run() {

        System.out.println("");
        System.out.println("Executing Default Micro Service ....");
        System.out.println("Work completed!");
    }
}
