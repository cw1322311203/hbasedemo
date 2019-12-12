package com.cw.bigdata.test;


public class TestClassLoader {
    public static void main(String[] args) {
        // TODO 类加载器的双亲委派机制
        // 1) 启动类加载器 : null(不是由java实现的)
        //          启动类加载器加载java的核心类库,它负责将 jdk目录/jre/lib 下面的类库或者 jdk目录/jre/classes 下的classes文件加载到内存中。
        //          由于引导类加载器涉及到虚拟机本地实现细节，开发者无法直接获取到启动类加载器的引用，所以不允许直接通过引用进行操作。
        // 2) 扩展类加载器 : sun.misc.Launcher$ExtClassLoader
        //          扩展类加载器加载java扩展类库
        //          它负责将 jdk目录/jre/lib/ext 下的jar包或者 jdk目录/jre/lib/ext/classes 下的classes文件
        //          或者由系统变量 java.ext.dir 指定位置中的类库加载到内存中。
        //          开发者可以直接使用标准扩展类加载器。
        // 3) 应用类加载器 : sun.misc.Launcher$AppClassLoader
        //          应用类加载器负责将系统类路径（CLASSPATH:指向当前目录./,如java代码在哪执行,哪里就是classpath）中指定的类库加载到内存中。
        //          开发者可以直接使用系统类加载器。
        //          自己写的类一般都是应用类加载器加载的

        // TODO 双亲委派机制工作流程
        //  如果一个类加载器收到了一个类加载的请求，它首先不会去加载类，而是去把这个请求委派给父加载器去加载，
        //  直到顶层启动类加载器，如果父类加载不了（不在父类加载的搜索范围内），才会自己去加载。


        // 不同的类加载器加载的位置不一样,所以如果不同的位置有相同的类,那么会遵循双亲委派机制


        System.out.println(TestClassLoader.class.getClassLoader());//sun.misc.Launcher$AppClassLoader@18b4aac2
        System.out.println(TestClassLoader.class.getClassLoader().getParent());//sun.misc.Launcher$ExtClassLoader@179d3b25
        System.out.println(TestClassLoader.class.getClassLoader().getParent().getParent());// null

        System.out.println(String.class.getClassLoader());// null

    }
}
