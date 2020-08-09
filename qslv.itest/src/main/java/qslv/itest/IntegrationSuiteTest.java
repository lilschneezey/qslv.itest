package qslv.itest;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
@SelectPackages( "qslv.itest")
@IncludeClassNamePatterns("^(Itest_.*|.+[.$]Itest_.*)$")
class IntegrationSuiteTest {


}
