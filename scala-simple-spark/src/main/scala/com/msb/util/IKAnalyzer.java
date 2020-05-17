package com.msb.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 分词器
 */
public class IKAnalyzer {

    public static void main(String[] args) {
        IKAnalyzer ik = new IKAnalyzer();
        List<String> words = ik.segmentation("最重要的是一点，由于要计算收敛，不可能一直递归下， 要设置一个阈值，只要新旧PR值的差值的和，达到这个阈值，就判定为收敛。");
        for (String word : words) {
            System.out.println(word);
        }
    }

    /**
     * 分词器
     *
     * @param sentence 文本串
     * @return 文本串切分后的单词列表
     */
    public List<String> segmentation(String sentence) {
        ArrayList<String> words = new ArrayList<String>();
        StringReader sr = new StringReader(sentence);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        while (true) {
            try {
                if (!((lex = ik.next()) != null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            words.add(lex.getLexemeText());
        }
        return words;
    }
}
