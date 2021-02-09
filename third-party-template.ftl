<#function artifactFormat p>
    <#return " (" + p.groupId + ":" + p.artifactId + ":" + p.version + ")">
</#function>

<section>
<#if licenseMap?size == 0>
The project has no dependencies.
<#else>
    <h3>List of third-party dependencies grouped by their license type.</h3>
    <#list licenseMap as e>
        <#assign license = e.getKey()/>
        <#assign projects = e.getValue()/>
        <#if projects?size &gt; 0>
   <h4>${license}:</h4>
        <#list projects as project>
           <span><a href="${project.url!"#"}">${project.name}</a>${artifactFormat(project)}</span><br/>
        </#list>
        </#if>
        <br/>
    </#list>
</#if>

</section>