package com.msb.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


public class IKAnalyzer {

    public List<String> segmentation(String sentence){
        ArrayList<String> words = new ArrayList<String>();
        StringReader sr=new StringReader(sentence);
        IKSegmenter ik=new IKSegmenter(sr, true);
        Lexeme lex=null;
        while(true) {
            try {
                if (!((lex=ik.next())!=null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            words.add(lex.getLexemeText());
        }
        return words;
    }
}
