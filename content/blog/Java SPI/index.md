---
title: Java SPI
date: "2023-03-01"
description: Java SPI机制是“基于接口编程+策略模式+配置文件”组合实现的动态加载机制，通过接口（Interface）寻找实现（Implement）并利用ServiceLoader来加载并实例化类，适用于根据需要使用、扩展或替换实现策略
tags: Java SPI
---

```toc
ordered: true
class-name: "table-of-contents"
```

Java SPI（Service Provider Interface）机制实际上是“基于接口编程+策略模式+配置文件”组合实现的动态加载机制，通过接口（Interface）寻找实现（Implement）并利用ServiceLoader来加载并实例化类，适用于根据需要使用、扩展或替换实现策略，常用来启用框架扩展和替换组件。

使用Java SPI需要符合的约定：
	1. Service provider提供接口的具体实现后，在目录META-INF/services下的文件（以Interface全路径命名）中添加具体实现类的全路径名
	2. 接口实现类的Jar包存放在使用程序的类路径（classpath）中
	3. 使用程序使用ServiceLoader动态加载实现类（根据目录META-INF/services下的配置文件找到实现类的全限定名并调用classloader来加载实现类到JVM
	4. SPI的实现必须具有无参数的构造方法

## 使用示例

```Java
// 先定义接口
public interface Search {
    public List<String> searchDoc(String keyword);
}

// 文件搜索实现
public class FileSearch implements Search{
    @Override
    public List<String> searchDoc(String keyword) {
        System.out.println("文件搜索 "+keyword);
        return null;
    }
}

// 数据库搜索实现
public class DatabaseSearch implements Search{
    @Override
    public List<String> searchDoc(String keyword) {
        System.out.println("数据搜索 "+keyword);
        return null;
    }
}

// 在resources目录下新建META-INF/services/目录，然后新建接口全限定名的文件：com.cainiao.ys.spi.learn.Search，里面加上需要用到的实现类

测试方法
public class TestCase {
    public static void main(String[] args) {
        ServiceLoader<Search> s = ServiceLoader.load(Search.class);
        Iterator<Search> iterator = s.iterator();
        while (iterator.hasNext()) {
           Search search =  iterator.next();
           search.searchDoc("hello world");
        }
    }
}

```

## ServiceLoader源码分析

```Java
// ServiceLoader主要用来完成对SPI的provider的加载
class ServiceLoader<S> implements Iterable<S> {
    String PREFIX = "META-INF/services/";   // 查找具体实现类的目录
    Class<S> service;                       // 正在加载的服务的类或接口
    ClassLoader loader;                     // 使用的类加载器
    AccessControlContext acc;               // 创建ServiceLoader时获取的安全策略
    LinkedHashMap<String,S> providers = new LinkedHashMap<>();      // 缓存provider
    LazyIterator lookupIterator;            // 用于懒加载（迭代时才加载）类的迭代器

    // 通过load方法获取实例
    public static <S> ServiceLoader<S> load(Class<S> service) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return ServiceLoader.load(service, cl);
    }

    public static <S> ServiceLoader<S> load(Class<S> service, ClassLoader loader) {
        return new ServiceLoader<>(service, loader);
    }

    private ServiceLoader(Class<S> svc, ClassLoader cl) {
        service = svc;
        loader = (cl == null) ? ClassLoader.getSystemClassLoader() : cl;
        acc = (System.getSecurityManager() != null) ? AccessController.getContext() : null;
        reload();
    }

    public void reload() {
        providers.clear();
        lookupIterator = new LazyIterator(service, loader);
    }

    public Iterator<S> iterator() {

        return new Iterator<S>() {

            Iterator<Map.Entry<String,S>> knownProviders = providers.entrySet().iterator();

            public boolean hasNext() {
                if (knownProviders.hasNext())
                    return true;
                return lookupIterator.hasNext();
            }

            public S next() {
                if (knownProviders.hasNext())
                    return knownProviders.next().getValue();
                return lookupIterator.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    // ServiceLoader的实际加载过程由LazyIterator完成
    private class LazyIterator implements Iterator<S> {

        Class<S> service;
        ClassLoader loader;
        Enumeration<URL> configs = null;       // 服务的URL枚举
        Iterator<String> pending = null;       // 要加载的服务的名称集合
        String nextName = null;                // 下一个要加载的服务名称

        private LazyIterator(Class<S> service, ClassLoader loader) {
            this.service = service;
            this.loader = loader;
        }

        public S next() {
            // 判断acc是否为空，若为空则说明没有设置安全策略直接调用nextService方法，否则以特权方式调用nextService方法
            if (acc == null) {
                return nextService();
            } else {
                PrivilegedAction<S> action = new PrivilegedAction<S>() {
                    public S run() { return nextService(); }
                };
                return AccessController.doPrivileged(action, acc);
            }
        }

        private S nextService() {

            String cn = nextName;
            nextName = null;
            Class<?> c = null;
            c = Class.forName(cn, false, loader);
            S p = service.cast(c.newInstance());
            providers.put(cn, p);
            return p;
        }

        private boolean hasNextService() {
            if (nextName != null) {    // 如果nextName不为空，则说明已经初始化过了，直接返回true
                return true;
            }

            if (configs == null) {
                // configs为空时进行初始化，根据PREFIX前缀加上PREFIX的全名得到完整路径，再通过类加载器获取URL的枚举
                String fullName = PREFIX + service.getName();
                if (loader == null)
                    configs = ClassLoader.getSystemResources(fullName);
                else
                    configs = loader.getResources(fullName);
            }
            // 完成pending的初始化或者添加
            while ((pending == null) || !pending.hasNext()) {
                if (!configs.hasMoreElements()) {
                    return false;
                }
                pending = parse(service, configs.nextElement());
            }
            nextName = pending.next();
            return true;
        }

        public boolean hasNext() {
            if (acc == null) {
                return hasNextService();
            } else {
                PrivilegedAction<Boolean> action = new PrivilegedAction<Boolean>() {
                    public Boolean run() { return hasNextService(); }
                };
                return AccessController.doPrivileged(action, acc);
            }
        }
    }

    // 通过URL打开输入流，通过parseLine一行一行地读取将结果保存在names数组里
    private Iterator<String> parse(Class<?> service, URL u) {
        InputStream in = null;
        BufferedReader r = null;
        ArrayList<String> names = new ArrayList<>();
        in = u.openStream();
        r = new BufferedReader(new InputStreamReader(in, "utf-8"));
        int lc = 1;
        while ((lc = parseLine(service, u, r, lc, names)) >= 0);
        if (r != null) r.close();
        if (in != null) in.close();
        return names.iterator();
    }
    private int parseLine(Class<?> service, URL u, BufferedReader r, int lc, List<String> names) {
        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }
        int ci = ln.indexOf('#');
        if (ci >= 0) ln = ln.substring(0, ci);
        ln = ln.trim();
        int n = ln.length();
        int cp = ln.codePointAt(0);
        for (int i = Character.charCount(cp); i < n; i += Character.charCount(cp)) {
            cp = ln.codePointAt(i);
            if (!providers.containsKey(ln) && !names.contains(ln))
                names.add(ln);
        }
        return lc + 1;
    }

}
```
