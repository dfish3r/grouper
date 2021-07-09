package edu.internet2.middleware.grouper.pspng;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.unboundid.ldap.sdk.DN;
import edu.internet2.middleware.morphString.Morph;
import org.apache.commons.lang.StringUtils;
import org.ldaptive.*;
import org.ldaptive.ad.handler.RangeEntryHandler;
import org.ldaptive.control.util.PagedResultsClient;
import org.ldaptive.handler.LdapEntryHandler;
import org.ldaptive.pool.BlockingConnectionPool;
import org.ldaptive.props.BindConnectionInitializerPropertySource;
import org.ldaptive.props.ConnectionConfigPropertySource;
import org.ldaptive.props.DefaultConnectionFactoryPropertySource;
import org.ldaptive.props.PooledConnectionFactoryPropertySource;
import org.ldaptive.props.SearchRequestPropertySource;
import org.ldaptive.sasl.Mechanism;
import org.ldaptive.sasl.SaslConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;
import edu.internet2.middleware.grouper.util.GrouperUtil;
import static edu.internet2.middleware.grouper.pspng.PspUtils.*;

/**
 * This class encapsulates an LDAP system configured by a collection of
 * properties defined withing grouper-loader.properties
 * @author bert
 *
 */
public class LdapSystem {
  private static final Logger LOG = LoggerFactory.getLogger(LdapSystem.class);

  // What ldaptive properties will be decrypted if their values are Morph files?
  // (We don't decrypt all properties because that would prevent the use of slashes in the property values)
  public static final String ENCRYPTABLE_LDAPTIVE_PROPERTIES[]
          = new String[]{"org.ldaptive.bindCredential"};

  public final String ldapSystemName;
  protected Properties _ldaptiveProperties = new Properties();
  
  private final boolean isActiveDirectory;
  private PooledConnectionFactory ldapPool;

  protected boolean searchResultPagingEnabled_defaultValue = true;
  protected int searchResultPagingSize_default_value = 100;


  public static boolean attributeHasNoValues(final LdapAttribute attribute) {
    if ( attribute == null ) {
      return true;
    }

    Collection<String> values = attribute.getStringValues();

    return values.size() == 0  || values.iterator().next().length() == 0;
  }


  public LdapSystem(String ldapSystemName, boolean isActiveDirectory) {
    this.ldapSystemName = ldapSystemName;
    this.isActiveDirectory = isActiveDirectory;
    getLdaptiveProperties();
  }

  
  private PooledConnectionFactory buildLdapConnectionFactory() throws PspException {
    PooledConnectionFactory result;
  
    LOG.info("{}: Creating LDAP Pool", ldapSystemName);
    Properties ldaptiveProperties = getLdaptiveProperties();

    Properties loggableProperties = new Properties();
    loggableProperties.putAll(ldaptiveProperties);

    for ( String propertyToMask : ENCRYPTABLE_LDAPTIVE_PROPERTIES )
    {
      if ( loggableProperties.containsKey(propertyToMask) )
      {
        loggableProperties.put(propertyToMask, "**masked**");
      }
    }
    
    LOG.info("Setting up LDAP Connection with properties: {}", loggableProperties);

    // Setup ldaptive ConnectionConfig
    ConnectionConfig connConfig = new ConnectionConfig();
    ConnectionConfigPropertySource ccpSource = new ConnectionConfigPropertySource(connConfig, ldaptiveProperties);
    ccpSource.initialize();
  
    //GrouperLoaderLdapServer grouperLoaderLdapProperties 
    //  = GrouperLoaderConfig.retrieveLdapProfile(ldapSystemName);
    
    /////////////
    // Binding
    BindConnectionInitializer binder = new BindConnectionInitializer();
  
    BindConnectionInitializerPropertySource bcip = new BindConnectionInitializerPropertySource(binder, ldaptiveProperties);
    bcip.initialize();
  
    // I'm not sure if SaslRealm and/or SaslAuthorizationId can be used independently
    // Therefore, we'll initialize gssApiConfig when either one of them is used.
    // And, then, we'll attach the gssApiConfig to the binder if there is a gssApiConfig
    SaslConfig saslConfig = null;
    String val = (String) ldaptiveProperties.get("org.ldaptive.saslRealm");
    if (!StringUtils.isBlank(val)) {
      LOG.info("Processing saslRealm");
      if ( saslConfig == null )
        saslConfig = SaslConfig.builder().mechanism(Mechanism.GSSAPI).build();
      saslConfig.setRealm(val);
    }
    
    val = (String) ldaptiveProperties.get("org.ldaptive.saslAuthorizationId");
    if (!StringUtils.isBlank(val)) {
      LOG.info("Processing saslAuthorizationId");
      if ( saslConfig == null )
        saslConfig = SaslConfig.builder().mechanism(Mechanism.GSSAPI).build();
      saslConfig.setAuthorizationId(val);
    }
  
    // If there was a sasl/gssapi attribute, then save the gssApiConfig
    if ( saslConfig != null ) {
      LOG.info("Setting gssApiConfig");
      binder.setBindSaslConfig(saslConfig);
    }
    
    PooledConnectionFactory connectionFactory = new PooledConnectionFactory();
    PooledConnectionFactoryPropertySource dcfSource = new PooledConnectionFactoryPropertySource(connectionFactory, ldaptiveProperties);
    dcfSource.initialize();

    // Test the ConnectionFactory before error messages are buried behind the pool
    performTestLdapRead(connectionFactory);
    
    /////////////
    // PoolConfig
    
    // Make sure some kind of validation is turned on
    if ( !connectionFactory.isValidateOnCheckIn() &&
         !connectionFactory.isValidateOnCheckOut() &&
         !connectionFactory.isValidatePeriodically() ) {
      LOG.debug("{}: Using default onCheckOut ldap-connection validation", ldapSystemName);
      connectionFactory.setValidateOnCheckOut(true);
    }
      
    connectionFactory.setValidator(new SearchConnectionValidator());
    connectionFactory.initialize();
    
    ////////////
    // Test the connection obtained from pool
    performTestLdapRead(connectionFactory);

    return connectionFactory;
  }

  public void log(LdapEntry ldapEntry, String ldapEntryDescriptionFormat, Object... ldapEntryDescriptionArgs)  {
    String ldapEntryDescription;
    if (LOG.isInfoEnabled() || LOG.isDebugEnabled()) {
      ldapEntryDescription = String.format(ldapEntryDescriptionFormat, ldapEntryDescriptionArgs);
    } else {
      return;
    }

    // INFO log is a count of each attribute's values
    if ( LOG.isInfoEnabled() ) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("dn=%s|", ldapEntry.getDn()));

      for (LdapAttribute attribute : ldapEntry.getAttributes()) {
        sb.append(String.format("%d %s values|", attribute.size(), attribute.getName()));
      }
      LOG.info("{}: {} Entry Summary: {}", ldapSystemName, ldapEntryDescription, sb.toString());
    }

    LOG.debug("{}: {} Entry Details: {}", ldapSystemName, ldapEntryDescription, ldapEntry);
  }

  public void log( ModifyRequest modifyRequest, String descriptionFormat, Object... descriptionArgs)  {
    String ldapEntryDescription;
    if (LOG.isInfoEnabled() || LOG.isDebugEnabled()) {
      ldapEntryDescription = String.format(descriptionFormat, descriptionArgs);
    } else {
      return;
    }

    // INFO log is a count of each attribute's values
    if ( LOG.isInfoEnabled() ) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("dn=%s|", modifyRequest.getDn()));

      for (AttributeModification mod : modifyRequest.getModifications()) {
        sb.append(String.format("%s %d %s values|",
                mod.getOperation(), mod.getAttribute().size(), mod.getAttribute().getName()));
      }
      LOG.info("{}: {} Mod Summary: {}", ldapSystemName, ldapEntryDescription, sb.toString());
    }

    LOG.debug("{}: {} Mod Details: {}", ldapSystemName, ldapEntryDescription, modifyRequest);
  }


  protected void performTestLdapRead(ConnectionFactory connectionFactory) throws PspException {
    LOG.info("{}: Performing test read of directory root", ldapSystemName);
    SearchRequest searchRequest = new SearchRequest();
    SearchRequestPropertySource srSource = new SearchRequestPropertySource(searchRequest, getLdaptiveProperties());
    srSource.initialize();
    searchRequest.setBaseDn("");
    searchRequest.setFilter("(objectclass=*)");
    searchRequest.setSearchScope(SearchScope.OBJECT);

    SearchOperation searchOp = new SearchOperation(connectionFactory);

    // Turn on attribute-value paging if this is an active directory target
    if ( isActiveDirectory() )
      searchOp.setSearchResultHandlers(new RangeEntryHandler());

    try {
      SearchResponse response = searchOp.execute(searchRequest);
      LdapEntry searchResultEntry = response.getEntry();
      log(searchResultEntry, "Ldap test success");
    }
    catch (LdapException e) {
      LOG.error("Ldap problem",e);
      throw new PspException("Problem testing ldap connection: %s", e.getMessage());
    }
  }

  
  
  public synchronized ConnectionFactory getLdapConnectionFactory() throws PspException {
    if ( ldapPool != null )
      return ldapPool;

    ldapPool = buildLdapConnectionFactory();
    return ldapPool;
  }

  
  
  public boolean isActiveDirectory() {
    return isActiveDirectory;
  }

  
  
  public Properties getLdaptiveProperties() {
    if ( _ldaptiveProperties.size() == 0 ) {
      String ldapPropertyPrefix = "ldap." + ldapSystemName.toLowerCase() + ".";
      
      for (String propName : GrouperLoaderConfig.retrieveConfig().propertyNames()) {
        if ( propName.toLowerCase().startsWith(ldapPropertyPrefix) ) {
          String propValue = GrouperLoaderConfig.retrieveConfig().propertyValueString(propName, "");
          
          // Get the part of the property after ldapPropertyPrefix 'ldap.person.'
          String propNameTail = propName.substring(ldapPropertyPrefix.length());
          _ldaptiveProperties.put("org.ldaptive." + propNameTail, propValue);
          
          // Some compatibility between old vtldap properties and ldaptive versions
          // url (vtldap) ==> ldapUrl
          if ( propNameTail.equalsIgnoreCase("url") ) {
            LOG.info("Setting org.ldaptive.ldapUrl");
            _ldaptiveProperties.put("org.ldaptive.ldapUrl", propValue);
          }
          // tls (vtldap) ==> useStartTls
          if ( propNameTail.equalsIgnoreCase("tls") ) {
            LOG.info("Setting org.ldaptive.useStartTLS");
            _ldaptiveProperties.put("org.ldaptive.useStartTLS", propValue);
          }
          // user (vtldap) ==> bindDn
          if ( propNameTail.equalsIgnoreCase("user") )
          {
            LOG.info("Setting org.ldaptive.bindDn");
            _ldaptiveProperties.put("org.ldaptive.bindDn", propValue);
          }
          // pass (vtldap) ==> bindCredential
          if ( propNameTail.equalsIgnoreCase("pass") )
          {
            LOG.info("Setting org.ldaptive.bindCredential");
            _ldaptiveProperties.put("org.ldaptive.bindCredential", propValue);
          }
        }
      }
    }

    // Go through the properties that can be encrypted and decrypt them if they're Morph files
    for (String encryptablePropertyKey : ENCRYPTABLE_LDAPTIVE_PROPERTIES) {
      String value = _ldaptiveProperties.getProperty(encryptablePropertyKey);
      value = Morph.decryptIfFile(value);
      _ldaptiveProperties.put(encryptablePropertyKey, value);
    }
    return _ldaptiveProperties;
  }

  
  
  public int getSearchResultPagingSize() {
    Object searchResultPagingSize = getLdaptiveProperties().get("org.ldaptive.searchResultPagingSize");
    
    return GrouperUtil.intValue(searchResultPagingSize, searchResultPagingSize_default_value);
  }

  
  
  public boolean isSearchResultPagingEnabled() {
    Object searchResultPagingEnabled = getLdaptiveProperties().get("org.ldaptive.searchResultPagingEnabled");
    
    return GrouperUtil.booleanValue(searchResultPagingEnabled, searchResultPagingEnabled_defaultValue);
  }



  /**
   * Returns ldaptive search operation configured according to properties
   * @return
   */
  public SearchOperation getSearchOperation() {
    SearchRequest searchRequest = new SearchRequest();
    SearchRequestPropertySource srSource = new SearchRequestPropertySource(searchRequest, getLdaptiveProperties());
    srSource.initialize();

    SearchOperation searchOperation = new SearchOperation();
    searchOperation.setRequest(searchRequest);
    return searchOperation;
  }

  
  
  protected void performLdapAdd(LdapEntry entryToAdd) throws PspException {
    log(entryToAdd, "Creating LDAP object");

    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      // Actually ADD the object
      AddOperation add = new AddOperation(connectionFactory);
      add.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));
      add.execute(new AddRequest(entryToAdd.getDn(), entryToAdd.getAttributes()));
    } catch (LdapException e) {
      if ( e.getResultCode() == ResultCode.ENTRY_ALREADY_EXISTS ) {
        LOG.warn("{}: Skipping LDAP ADD because object already existed: {}", ldapSystemName, entryToAdd.getDn());
      } else {
        LOG.error("{}: Problem while creating new ldap object: {}",
                new Object[] {ldapSystemName, entryToAdd, e});

        throw new PspException("LDAP problem creating object: %s", e.getMessage());
      }
    }
  }
  
  

  protected void performLdapDelete(String dnToDelete) throws PspException {
    LOG.info("{}: Deleting LDAP object: {}", ldapSystemName, dnToDelete);
    
    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      // Actually DELETE the account
      DeleteOperation delete = new DeleteOperation(connectionFactory);
      delete.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));
      delete.execute(new DeleteRequest(dnToDelete));
    } catch (LdapException e) {
      LOG.error("Problem while deleting ldap object: {}", dnToDelete, e);
      throw new PspException("LDAP problem deleting object: %s", e.getMessage());
    }
  }

  public void performLdapModify(ModifyRequest mod, boolean valuesAreCaseSensitive) throws PspException {
    performLdapModify(mod, valuesAreCaseSensitive,true);
  }

  /**
   * This performs a modification and optionally retries it by comparing attributeValues
   * being added/removed to those already on the ldap server
   * @param mod
   * @param retryIfFails Should the Modify be retried if something goes wrong. This retry
   *                     will do attributeValue-by-attributeValue comparison to
   *                     make the retry as safe as possible
   * @throws PspException
   */
  public void performLdapModify(ModifyRequest mod, boolean valuesAreCaseSensitive, boolean retryIfFails) throws PspException {
    log(mod, "Performing ldap mod (%s retry)", retryIfFails ? "with" : "without");

    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      ModifyOperation modify = new ModifyOperation(connectionFactory);
      modify.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));
      modify.execute(mod);
    } catch (LdapException e) {

      // Abort with Exception if retries are disabled
      if ( !retryIfFails ) {
        throw new PspException("%s: Unrecoverable problem modifying ldap object: %s %s",
                ldapSystemName, mod, e.getMessage());
      }


      LOG.warn("{}: Problem while modifying ldap system based on grouper expectations. Starting to perform adaptive modifications based on data already on server: {}: {}",
              new Object[]{ldapSystemName, mod.getDn(), e.getResultCode()});

      // First case: a single attribute being modified with a single value
      //   Perform a quick ldap comparison and check to see if the object
      //   already matches the modification
      //
      //   If the object doesn't already match, then it was a real ldap failure... there
      //   is no way to simplify it or otherwise retry it
      if ( mod.getModifications().length == 1 &&
           mod.getModifications()[0].getAttribute().getStringValues().size() == 1 ) {
        AttributeModification modification = mod.getModifications()[0];

        boolean attributeMatches = performLdapComparison(mod.getDn(), modification.getAttribute());

        if ( attributeMatches && modification.getOperation() == AttributeModification.Type.ADD ) {
          LOG.info("{}: Change not necessary: System already had attribute value", ldapSystemName);
          return;
        }
        else if ( !attributeMatches && modification.getOperation() == AttributeModification.Type.DELETE ) {
          LOG.info("{}: Change not necessary: System already had attribute value removed", ldapSystemName);
          return;
        }
        else {
          LOG.error("{}: Single-attribute-value Ldap mod-{} failed when Ldap server {} already have {}={}. Mod that failed: {}",
                  ldapSystemName, modification.getOperation().toString().toLowerCase(),
                  attributeMatches ? "does" : "does not",
                  modification.getAttribute().getName(), modification.getAttribute().getStringValue(),
                  mod,
                  e);
          throw new PspException("LDAP Modification Failed");
        }
      }

      // This wasn't a single-attribute change, or multiple values were being changed.
      // Therefore: Read what is in the LDAP server and implement the differences


      // Gather up the attributes that were modified so we can read them from server
      Set<String> attributeNames = new HashSet<>();
      for ( AttributeModification attributeMod : mod.getModifications()) {
        attributeNames.add(attributeMod.getAttribute().getName());
      }

      // Read the current values of those attributes
      LOG.info("{}: Modification retrying... reading object to know what needs to change: {}",
        ldapSystemName, mod.getDn());

      LdapObject currentLdapObject = performLdapRead(mod.getDn(), attributeNames);
      log(currentLdapObject.ldapEntry, "Data already on ldap server");

      // Go back through the requested mods and see if they are redundant
      for ( AttributeModification attributeMod : mod.getModifications()) {
        String attributeName = attributeMod.getAttribute().getName();

        LOG.info("{}: Summary: Comparing modification of {} to what is already in LDAP: {}/{} Values",
                ldapSystemName,
                attributeName,
                attributeMod.getOperation(),
                attributeMod.getAttribute().size());
        LOG.debug("{}: Details: Comparing modification of {} to what is already in LDAP: {}/{}",
                ldapSystemName,
                attributeName,
                attributeMod.getOperation(),
                attributeMod.getAttribute());

        Collection<String> currentValues = currentLdapObject.getStringValues(attributeName);
        Collection<String> modifyValues  = attributeMod.getAttribute().getStringValues();

        LOG.info("{}: Comparing Attribute {}. #Values on server already {}. #Values in mod/{}: {}",
          ldapSystemName, attributeName, currentValues.size(), attributeMod.getOperation(), modifyValues.size());

        switch (attributeMod.getOperation()) {
          case ADD:
            // See if any modifyValues are missing from currentValues
            //
            // Subtract currentValues from modifyValues (case-insensitively)
            Set<String> valuesNotAlreadyOnServer =
                    subtractStringCollections(valuesAreCaseSensitive, modifyValues, currentValues);

            LOG.debug("{}: {}: Values on server: {}",
                    ldapSystemName, attributeName, currentValues);
            LOG.debug("{}: {}: Modify/Add values: {}",
                    ldapSystemName, attributeName, modifyValues);

            LOG.info("{}: {}: Need to add {} values",
                    ldapSystemName, attributeName, valuesNotAlreadyOnServer.size());

            for ( String valueToChange : valuesNotAlreadyOnServer ) {
              performLdapModify( new ModifyRequest( mod.getDn(),
                      new AttributeModification(AttributeModification.Type.ADD,
                              new LdapAttribute(attributeName, valueToChange))),
                      valuesAreCaseSensitive,false);
            }
            break;

          case DELETE:
            // For Mod.DELETE, not specifying any values means to remove them all
            if ( modifyValues.size() == 0 ) {
              modifyValues.addAll(currentValues);
            }

            // See if any modifyValues are still in currentValues
            //
            // Intersect modifyValues and currentValues
            Set<String> valuesStillOnServer
                    = intersectStringCollections(valuesAreCaseSensitive, modifyValues, currentValues);
            LOG.debug("{}: {}: Values on server: {}",
                    ldapSystemName, attributeName, currentValues);
            LOG.debug("{}: {}: Modify/Delete values: {}",
                    ldapSystemName, attributeName, modifyValues);

            LOG.info("{}: {}: {} values need to be REMOVEd",
                    ldapSystemName, attributeName, valuesStillOnServer.size());

            for (String valueToChange : valuesStillOnServer) {
              performLdapModify(new ModifyRequest(mod.getDn(),
                              new AttributeModification(AttributeModification.Type.DELETE,
                                      new LdapAttribute(attributeName, valueToChange))),
                      valuesAreCaseSensitive,false);
            }
            break;

          case REPLACE:
            // See if any differences between modifyValues and currentValues
            // (Subtract in both directions)

            LOG.debug("{}: {}: Values on server: {}",
                    ldapSystemName, attributeName, currentValues);
            LOG.debug("{}: {}: Modify/Replace values: {}",
                    ldapSystemName, attributeName, modifyValues);

            Set<String> extraValuesOnServer =
                    subtractStringCollections(valuesAreCaseSensitive, currentValues, modifyValues);
            LOG.info("{}: REPLACE: {}: {} values still need to be REMOVEd",
                    ldapSystemName, attributeNames, extraValuesOnServer.size());

            for (String valueToChange : extraValuesOnServer) {
              performLdapModify(new ModifyRequest(mod.getDn(),
                              new AttributeModification(AttributeModification.Type.DELETE,
                                      new LdapAttribute(attributeName, valueToChange))),
                      valuesAreCaseSensitive,false);
            }

            Set<String> missingValuesOnServer =
                    subtractStringCollections(valuesAreCaseSensitive, modifyValues, currentValues);

            LOG.info("{}: REPLACE: {}: {} values need to be ADDed",
                    ldapSystemName, attributeName, missingValuesOnServer.size());

            for ( String valueToChange : missingValuesOnServer ) {
              performLdapModify( new ModifyRequest( mod.getDn(),
                              new AttributeModification(AttributeModification.Type.ADD,
                                      new LdapAttribute(attributeName, valueToChange))),
                      valuesAreCaseSensitive, false);
            }
        }
      }
    }
  }

  private boolean performLdapComparison(String dn, LdapAttribute attribute) throws PspException {
    LOG.info("{}: Performaing Ldap comparison operation: {} on {}",
            new Object[]{ldapSystemName, attribute, LdapObject.getDnSummary(dn,2)});

    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      CompareOperation compare = new CompareOperation(connectionFactory);
      compare.setThrowCondition(result ->
        !result.getResultCode().equals(ResultCode.COMPARE_TRUE) &&
          !result.getResultCode().equals(ResultCode.COMPARE_FALSE));
      CompareResponse result = compare.execute(new CompareRequest(dn, attribute.getName(), attribute.getStringValue()));
      return result.isTrue();

    } catch (LdapException ldapException) {
      ResultCode resultCode = ldapException.getResultCode();

      // A couple errors mean that object does not match attribute values
      if (resultCode == ResultCode.NO_SUCH_OBJECT || resultCode == ResultCode.NO_SUCH_ATTRIBUTE) {
        return false;
      } else {
        LOG.error("{}: Error performing compare operation: {}",
          new Object[]{ldapSystemName, attribute, ldapException});

        throw new PspException("LDAP problem performing ldap comparison: %s", ldapException.getMessage());
      }
    }
  }


  void performLdapModifyDn(ModifyDnRequest mod) throws PspException {
    LOG.info("{}: Performing Ldap mod-dn operation: {}", ldapSystemName, mod);

    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      ModifyDnOperation modifyDn = new ModifyDnOperation(connectionFactory);
      modifyDn.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));
      modifyDn.execute(mod);
    } catch (LdapException e) {
      LOG.error("Problem while modifying dn of ldap object: {}", mod, e);
      throw new PspException("LDAP problem modifying dn of ldap object: %s", e.getMessage());
    }
  }




  protected LdapObject performLdapRead(DN dn, String... attributes) throws PspException {
    return performLdapRead(dn.toMinimallyEncodedString(), attributes);
  }
  
  protected LdapObject performLdapRead(String dn, Collection<String> attributes) throws PspException {
    return performLdapRead(dn, attributes.toArray(new String[0]));
  }

  protected LdapObject performLdapRead(String dn, String... attributes) throws PspException {
    LOG.debug("Doing ldap read: {} attributes {}", dn, Arrays.toString(attributes));
    
    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      SearchRequest read = new SearchRequest(dn, "(objectclass=*)");
      read.setSearchScope(SearchScope.OBJECT);
      read.setReturnAttributes(attributes);

      SearchOperation searchOp = new SearchOperation(connectionFactory);
      searchOp.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));

      // Turn on attribute-value paging if this is an active directory target
      if ( isActiveDirectory() ) {
        LOG.info("Active Directory: Searching with Ldap RangeEntryHandler");
        searchOp.setSearchResultHandlers(new RangeEntryHandler());
      }

      SearchResponse response = searchOp.execute(read);
      LdapEntry result = response.getEntry();
      
      if ( result == null ) {
        LOG.debug("{}: Object does not exist: {}", ldapSystemName, dn);
        return null;
      } else {
        LOG.debug("{}: Object does exist: {}", ldapSystemName, dn);
        return new LdapObject(result, attributes);
      }
    }
    catch (LdapException e) {
      if ( e.getResultCode() == ResultCode.NO_SUCH_OBJECT ) {
        LOG.warn("{}: Ldap object does not exist: '{}'", ldapSystemName, dn);
        return null;
      }
      
      LOG.error("Problem during ldap read {}", dn, e);
      throw new PspException("Problem during LDAP read: %s", e.getMessage());
    }
  }

  /**
   * 
   * @param request
   *
   * @return
   * @throws LdapException
   */
  protected void performLdapSearchRequest(int approximateNumResultsExpected, SearchRequest request, LdapEntryHandler callback) throws PspException {
    LOG.debug("Doing ldap search: {} / {} / {}", 
        new Object[] {request.getFilter(), request.getBaseDn(), Arrays.toString(request.getReturnAttributes())});

    ConnectionFactory connectionFactory = getLdapConnectionFactory();
    try {
      // Perform search. This is slightly different if paging is enabled or not.
      if ( isSearchResultPagingEnabled() ) {
        PagedResultsClient client = new PagedResultsClient(connectionFactory, getSearchResultPagingSize());
        // Turn on attribute-value paging if this is an active directory target
        if ( isActiveDirectory() ) {
          LOG.debug("Using attribute-value paging");
          client.setSearchResultHandlers(new RangeEntryHandler());
        } else {
          LOG.debug("Not using attribute-value paging");
        }
        client.setEntryHandlers(
          new LdapSearchProgressHandler(approximateNumResultsExpected, LOG, "Performing ldap search"),
          callback);
        LOG.debug("Using ldap search-result paging");
        client.executeToCompletion(request);
      }
      else {
        LOG.debug("Not using ldap search-result paging");
        SearchOperation searchOp = new SearchOperation(connectionFactory);
        searchOp.setThrowCondition(result -> !result.getResultCode().equals(ResultCode.SUCCESS));
        // Turn on attribute-value paging if this is an active directory target
        if ( isActiveDirectory() ) {
          LOG.debug("Using attribute-value paging");
          searchOp.setSearchResultHandlers(new RangeEntryHandler());
        } else {
          LOG.debug("Not using attribute-value paging");
        }
        searchOp.setEntryHandlers(
          new LdapSearchProgressHandler(approximateNumResultsExpected, LOG, "Performing ldap search"),
          callback);
        searchOp.execute(request);
      }
      
    }
    catch (LdapException e) {
      if ( e.getResultCode() == ResultCode.NO_SUCH_OBJECT ) {
        LOG.warn("Search base does not exist: {} (No such object ldap error)", request.getBaseDn());
        return;
      }
      
      LOG.error("Problem during ldap search {}", request, e);
      throw new PspException("LDAP problem while searching: " + e.getMessage());
    }
    catch (RuntimeException e) {
      LOG.error("Runtime problem during ldap search {}", request, e);
      throw e;
    }
  }



  public List<LdapObject> performLdapSearchRequest(int approximateNumResultsExpected, String searchBaseDn, SearchScope scope, Collection<String> attributesToReturn, String filterTemplate, Object... filterParams)
  throws PspException {
    FilterTemplate filter = new FilterTemplate(filterTemplate);

    for (int i=0; i<filterParams.length; i++) {
      filter.setParameter(i, filterParams[i]);
    }

    return performLdapSearchRequest(approximateNumResultsExpected, searchBaseDn, scope, attributesToReturn, filter);
  }


  public List<LdapObject> performLdapSearchRequest(int approximateNumResultsExpected, String searchBaseDn, SearchScope scope, Collection<String> attributesToReturn, FilterTemplate filter)
          throws PspException {
    LOG.debug("Running ldap search: <{}>/{}: {} << {}",
            searchBaseDn, scope, filter.getFilter(), filter.getParameters());

    final SearchRequest request = new SearchRequest(searchBaseDn, filter, attributesToReturn.toArray(new String[0]));
    request.setSearchScope(scope);


    final List<LdapObject> result = new ArrayList<>();
    LdapEntryHandler searchCallback = ldapEntry -> {
      LOG.debug("Ldap result: {}", ldapEntry.getDn());
      result.add(new LdapObject(ldapEntry, request.getReturnAttributes()));
      return null;
    };
    performLdapSearchRequest(approximateNumResultsExpected, request, searchCallback);

    LOG.info("LDAP search returned {} entries", result.size());

    if ( LOG.isTraceEnabled() ) {
      int i=0;
      for (LdapObject ldapObject : result ) {
        i++;
        LOG.trace("...ldap-search result {} of {}: {}", new Object[]{i, result.size(), ldapObject.getMap()});
      }
    }
    return result;

  }


  public Set<String> performLdapSearchRequest_returningValuesOfAnAttribute(int approximateNumResultsExpected, String searchBaseDn, SearchScope scope, final String attributeToReturn, String filterTemplate, Object... filterParams)
          throws PspException {
    FilterTemplate filter = new FilterTemplate(filterTemplate);
    LOG.debug("Running ldap search: <{}>/{}: {} << {}",
            new Object[]{searchBaseDn, scope, filterTemplate, Arrays.toString(filterParams)});

    for (int i=0; i<filterParams.length; i++) {
      filter.setParameter(i, filterParams[i]);
    }

    final SearchRequest request = new SearchRequest(searchBaseDn, filter, new String[]{attributeToReturn});
    request.setSearchScope(scope);


    // Create a place to hold the String-only results and a handler to put them into it
    final Set<String> result = new HashSet<>();
    LdapEntryHandler searchCallback = ldapEntry -> {
      if ( attributeToReturn.equalsIgnoreCase("dn") || attributeToReturn.equalsIgnoreCase("distinguishedName") ) {
        result.add(ldapEntry.getDn().toLowerCase());
      } else {
        LdapAttribute attribute = ldapEntry.getAttribute(attributeToReturn);
        if (attribute != null)
          result.addAll(attribute.getStringValues());
      }
      return null;
    };

    performLdapSearchRequest(approximateNumResultsExpected, request, searchCallback);

    LOG.info("LDAP search returned {} entries", result.size());

    if ( LOG.isTraceEnabled() ) {
      int i=0;
      for (String attributeValue : result ) {
        i++;
        LOG.trace("...ldap-search result {} of {}: {}", i, result.size(), attributeValue);
      }
    }
    return result;

  }


  public boolean makeLdapObjectCorrect(LdapEntry correctEntry,
                                         LdapEntry existingEntry,
                                       boolean valuesAreCaseSensitive)
          throws PspException
  {
    boolean changedDn = false, changedAttributes = false;

    changedDn = makeLdapDnCorrect(correctEntry, existingEntry);
    if ( changedDn ) {
      LOG.info("{}: Rereading entry after changing DN", ldapSystemName, correctEntry.getDn());

      LdapObject rereadLdapObject = performLdapRead(correctEntry.getDn(), getAttributeNames(existingEntry));

      // this should always be found, but checking just in case
      if ( rereadLdapObject!= null ) {
        existingEntry = rereadLdapObject.ldapEntry;
      }
    }

    changedAttributes = makeLdapDataCorrect(correctEntry, existingEntry, valuesAreCaseSensitive);

    return changedDn || changedAttributes;

/*
    if ( changed ) {
      return fetchTargetSystemGroup(grouperGroupInfo);
    }
    else {
      return existingGroup;
    }
*/
  }


  /**
   * Read a fresh copy of an ldapEntry, using the dn and attribute list from the provided
   * entry.
   *
   * @param ldapEntry Source of DN and attributes that should be read.
   * @return
   * @throws PspException
   */

  public LdapEntry rereadEntry(LdapEntry ldapEntry) throws PspException {
    Collection<String> attributeNames = getAttributeNames(ldapEntry);

    try {
      LOG.debug("{}: Rereading entry {}", ldapSystemName, ldapEntry.getDn());
      LdapObject result = performLdapRead(ldapEntry.getDn(), attributeNames);
      return result.ldapEntry;
    } catch (PspException e) {
      LOG.error("{} Unable to reread ldap object {}", ldapSystemName, ldapEntry.getDn(), e);
      throw e;
    }
  }

  /**
   * Get the names of the attributes present in a given LdapEntry
   * @param ldapEntry
   * @return
   */
  private Collection<String> getAttributeNames(LdapEntry ldapEntry) {
    Collection<String> attributeNames = new HashSet<>();

    for (LdapAttribute attribute : ldapEntry.getAttributes() ) {
      attributeNames.add(attribute.getName());
    }
    return attributeNames;
  }

  protected boolean makeLdapDataCorrect(LdapEntry correctEntry,
                                        LdapEntry existingEntry,
                                        boolean valuesAreCaseSensitive)
        throws PspException
  {
    boolean changed = false ;
    for ( String attributeName : correctEntry.getAttributeNames() ) {
      LdapAttribute correctAttribute = correctEntry.getAttribute(attributeName);
      if ( attributeHasNoValues(correctAttribute) ) {
        correctAttribute = null;
      }

      LdapAttribute existingAttribute= existingEntry.getAttribute(attributeName);

      // If there should not be any values for this attribute, delete any existing values
      if ( correctAttribute == null ) {
        if ( existingAttribute != null ) {
          changed = true;
          LOG.info("{}: Attribute {} is incorrect: {} current values, Correct values: none",
                  correctEntry.getDn(), attributeName,
                  (existingAttribute != null ? existingAttribute.size() : "<none>"));

          AttributeModification mod = new AttributeModification(AttributeModification.Type.DELETE, existingAttribute);
          ModifyRequest modRequest = new ModifyRequest(correctEntry.getDn(), mod);
          performLdapModify(modRequest, valuesAreCaseSensitive);
        }
      }
      else if ( !correctAttribute.equals(existingAttribute) ) {
        // Attribute is different. Update existing one
        changed = true;
        LOG.info("{}: Attribute {} is incorrect: {} Current values, {} Correct values",
                correctEntry.getDn(),
                attributeName,
                (existingAttribute != null ? existingAttribute.size() : "<none>"),
                (correctAttribute  != null ? correctAttribute.size() : "<none>" ));

        AttributeModification mod = new AttributeModification(AttributeModification.Type.REPLACE, correctAttribute);
        ModifyRequest modRequest = new ModifyRequest(correctEntry.getDn(), mod);
        performLdapModify(modRequest, valuesAreCaseSensitive);
      }
    }
    return changed;
  }

  /**
   * Moves the ldap object if necessary. It does require the OU to already exist because
   * OU templates and OU caching would make OU-creation here too intertwined with
   * the provisioning objects
   *
   * @param correctEntry
   * @param existingEntry
   * @return
   * @throws PspException
   */
  protected boolean makeLdapDnCorrect(LdapEntry correctEntry, LdapEntry existingEntry) throws PspException {
    // Compare DNs
    String correctDn = correctEntry.getDn();
    String existingDn= existingEntry.getDn();

    // TODO: This should do case-sensitive comparisons of the first RDN and case-insensitive comparisons of the rest
    if ( !correctDn.equalsIgnoreCase(existingDn) ) {
      // The DNs do not match. Existing object needs to be moved
      LOG.debug("{}: DN needs to change to {}", existingDn, correctDn);

      // Now modify the DN
      ModifyDnRequest moddn = new ModifyDnRequest(existingDn, correctDn, true);

      performLdapModifyDn(moddn);
      return true;
    }
    return false;
  }


  public boolean test() {
    String ldapUrlString = (String) getLdaptiveProperties().get("org.ldaptive.ldapUrl");
    if ( ldapUrlString == null ) {
      LOG.error("Could not find LDAP URL");
      return false;
    }
    
    LOG.info("LDAP Url: " + ldapUrlString);
    
    if ( !ldapUrlString.startsWith("ldaps") ) {
      LOG.warn("Not an SSL ldap url");
    }
    else {        
      LOG.info("Testing SSL before the LDAP test");
      try {
        // ldaps://host[:port]...
        Pattern urlPattern = Pattern.compile("ldaps://([^:]*)(:[0-9]+)?.*");
        Matcher m = urlPattern.matcher(ldapUrlString);
        if ( !m.matches() ) {
          LOG.error("Unable to parse ldap url: " + ldapUrlString);
          return false;
        }
        
        String host = m.group(1);
        String portString = m.group(2);
        int port;
        if ( portString == null || portString.length() == 0 ) {
          port=636;
        }
        else {
          port=Integer.parseInt(portString.substring(1));
        }
        
        LOG.info("  Making SSL connection to {}:{}", host, port);
        
        SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket sslsocket = (SSLSocket) sslsocketfactory.createSocket(host, port);

        InputStream in = sslsocket.getInputStream();
        OutputStream out = sslsocket.getOutputStream();

        // Write a test byte to get a reaction :)
        out.write(1);

        while (in.available() > 0) {
            System.out.print(in.read());
        }
        LOG.info("Successfully connected");

      } catch (Exception exception) {
          exception.printStackTrace();
      }
    }
    
    try {
      PooledConnectionFactory pool = buildLdapConnectionFactory();
      LOG.info("Success: Ldap pool built");

      performTestLdapRead(pool);
      LOG.info("Success: Test ldap read");
      return true;
    }
    catch (PspException e) {
      LOG.error("LDAP Failure",e);
      return false;
    }
  }

  public static void main(String[] args) {
    if ( args.length != 1 ) {
      LOG.error("USAGE: <ldap-pool-name from grouper-loader.properties>");
      System.exit(1);
    }
   
    LOG.info("Starting LDAP-connection test");
    LdapSystem system = new LdapSystem(args[0], false);
    system.test();
  }
}
