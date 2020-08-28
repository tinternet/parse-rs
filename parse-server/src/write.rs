use crate::error::Error;
// use crate::request::WriteRequest;
// use crate::schema::SchemaCache;
use crate::util;
use bson::Document;

// TODO: rewrite to rust
async fn handle_installation() -> Result<(), Error> {
    Ok(())
    // if (this.response || this.className !== '_Installation') {
    //     return;
    // }
    //   if (!this.query && !this.data.deviceToken && !this.data.installationId && !this.auth.installationId) {
    //     throw new Parse.Error(135, 'at least one ID field (deviceToken, installationId) ' + 'must be specified in this operation');
    //   } // If the device token is 64 characters long, we assume it is for iOS
    //   // and lowercase it.
    //   if (this.data.deviceToken && this.data.deviceToken.length == 64) {
    //     this.data.deviceToken = this.data.deviceToken.toLowerCase();
    //   } // We lowercase the installationId if present
    //   if (this.data.installationId) {
    //     this.data.installationId = this.data.installationId.toLowerCase();
    //   }
    //   let installationId = this.data.installationId; // If data.installationId is not set and we're not master, we can lookup in auth
    //   if (!installationId && !this.auth.isMaster) {
    //     installationId = this.auth.installationId;
    //   }
    //   if (installationId) {
    //     installationId = installationId.toLowerCase();
    //   } // Updating _Installation but not updating anything critical
    //   if (this.query && !this.data.deviceToken && !installationId && !this.data.deviceType) {
    //     return;
    //   }
    //   var promise = Promise.resolve();
    //   var idMatch; // Will be a match on either objectId or installationId
    //   var objectIdMatch;
    //   var installationIdMatch;
    //   var deviceTokenMatches = []; // Instead of issuing 3 reads, let's do it with one OR.
    //   const orQueries = [];
    //   if (this.query && this.query.objectId) {
    //     orQueries.push({
    //       objectId: this.query.objectId
    //     });
    //   }
    //   if (installationId) {
    //     orQueries.push({
    //       installationId: installationId
    //     });
    //   }
    //   if (this.data.deviceToken) {
    //     orQueries.push({
    //       deviceToken: this.data.deviceToken
    //     });
    //   }
    //   if (orQueries.length == 0) {
    //     return;
    //   }
    //   promise = promise.then(() => {
    //     return this.config.database.find('_Installation', {
    //       $or: orQueries
    //     }, {});
    //   }).then(results => {
    //     results.forEach(result => {
    //       if (this.query && this.query.objectId && result.objectId == this.query.objectId) {
    //         objectIdMatch = result;
    //       }
    //       if (result.installationId == installationId) {
    //         installationIdMatch = result;
    //       }
    //       if (result.deviceToken == this.data.deviceToken) {
    //         deviceTokenMatches.push(result);
    //       }
    //     }); // Sanity checks when running a query
    //     if (this.query && this.query.objectId) {
    //       if (!objectIdMatch) {
    //         throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found for update.');
    //       }
    //       if (this.data.installationId && objectIdMatch.installationId && this.data.installationId !== objectIdMatch.installationId) {
    //         throw new Parse.Error(136, 'installationId may not be changed in this ' + 'operation');
    //       }
    //       if (this.data.deviceToken && objectIdMatch.deviceToken && this.data.deviceToken !== objectIdMatch.deviceToken && !this.data.installationId && !objectIdMatch.installationId) {
    //         throw new Parse.Error(136, 'deviceToken may not be changed in this ' + 'operation');
    //       }
    //       if (this.data.deviceType && this.data.deviceType && this.data.deviceType !== objectIdMatch.deviceType) {
    //         throw new Parse.Error(136, 'deviceType may not be changed in this ' + 'operation');
    //       }
    //     }
    //     if (this.query && this.query.objectId && objectIdMatch) {
    //       idMatch = objectIdMatch;
    //     }
    //     if (installationId && installationIdMatch) {
    //       idMatch = installationIdMatch;
    //     } // need to specify deviceType only if it's new
    //     if (!this.query && !this.data.deviceType && !idMatch) {
    //       throw new Parse.Error(135, 'deviceType must be specified in this operation');
    //     }
    //   }).then(() => {
    //     if (!idMatch) {
    //       if (!deviceTokenMatches.length) {
    //         return;
    //       } else if (deviceTokenMatches.length == 1 && (!deviceTokenMatches[0]['installationId'] || !installationId)) {
    //         // Single match on device token but none on installationId, and either
    //         // the passed object or the match is missing an installationId, so we
    //         // can just return the match.
    //         return deviceTokenMatches[0]['objectId'];
    //       } else if (!this.data.installationId) {
    //         throw new Parse.Error(132, 'Must specify installationId when deviceToken ' + 'matches multiple Installation objects');
    //       } else {
    //         // Multiple device token matches and we specified an installation ID,
    //         // or a single match where both the passed and matching objects have
    //         // an installation ID. Try cleaning out old installations that match
    //         // the deviceToken, and return nil to signal that a new object should
    //         // be created.
    //         var delQuery = {
    //           deviceToken: this.data.deviceToken,
    //           installationId: {
    //             $ne: installationId
    //           }
    //         };
    //         if (this.data.appIdentifier) {
    //           delQuery['appIdentifier'] = this.data.appIdentifier;
    //         }
    //         this.config.database.destroy('_Installation', delQuery).catch(err => {
    //           if (err.code == Parse.Error.OBJECT_NOT_FOUND) {
    //             // no deletions were made. Can be ignored.
    //             return;
    //           } // rethrow the error
    //           throw err;
    //         });
    //         return;
    //       }
    //     } else {
    //       if (deviceTokenMatches.length == 1 && !deviceTokenMatches[0]['installationId']) {
    //         // Exactly one device token match and it doesn't have an installation
    //         // ID. This is the one case where we want to merge with the existing
    //         // object.
    //         const delQuery = {
    //           objectId: idMatch.objectId
    //         };
    //         return this.config.database.destroy('_Installation', delQuery).then(() => {
    //           return deviceTokenMatches[0]['objectId'];
    //         }).catch(err => {
    //           if (err.code == Parse.Error.OBJECT_NOT_FOUND) {
    //             // no deletions were made. Can be ignored
    //             return;
    //           } // rethrow the error
    //           throw err;
    //         });
    //       } else {
    //         if (this.data.deviceToken && idMatch.deviceToken != this.data.deviceToken) {
    //           // We're setting the device token on an existing installation, so
    //           // we should try cleaning out old installations that match this
    //           // device token.
    //           const delQuery = {
    //             deviceToken: this.data.deviceToken
    //           }; // We have a unique install Id, use that to preserve
    //           // the interesting installation
    //           if (this.data.installationId) {
    //             delQuery['installationId'] = {
    //               $ne: this.data.installationId
    //             };
    //           } else if (idMatch.objectId && this.data.objectId && idMatch.objectId == this.data.objectId) {
    //             // we passed an objectId, preserve that instalation
    //             delQuery['objectId'] = {
    //               $ne: idMatch.objectId
    //             };
    //           } else {
    //             // What to do here? can't really clean up everything...
    //             return idMatch.objectId;
    //           }
    //           if (this.data.appIdentifier) {
    //             delQuery['appIdentifier'] = this.data.appIdentifier;
    //           }
    //           this.config.database.destroy('_Installation', delQuery).catch(err => {
    //             if (err.code == Parse.Error.OBJECT_NOT_FOUND) {
    //               // no deletions were made. Can be ignored.
    //               return;
    //             } // rethrow the error
    //             throw err;
    //           });
    //         } // In non-merge scenarios, just return the installation match id
    //         return idMatch.objectId;
    //       }
    //     }
    //   }).then(objId => {
    //     if (objId) {
    //       this.query = {
    //         objectId: objId
    //       };
    //       delete this.data.objectId;
    //       delete this.data.createdAt;
    //     } // TODO: Validate ops (add/remove on channels, $inc on badge, etc.)
    //   });
    //   return promise;
}

async fn handle_session<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    if req.class_name != "_Session" {
        return Ok(());
    }

    if req.auth.user.is_none() && !req.auth.is_master {
        // TODO: map error
        return Err(Error::Forbidden("Session token required".to_string()));
        //   throw new Parse.Error(Parse.Error.INVALID_SESSION_TOKEN, 'Session
    }

    //   if this.data.ACL {
    // TODO: map error
    //   Err(Error::Forbidden("Session token required"));
    // throw new Parse.Error(Parse.Error.INVALID_KEY_NAME, 'Cannot set ' + 'ACL on a Session.');
    // }

    if req.query.is_some() {
        if !req.auth.is_master && req.auth.user.is_some() {
            let userId = util::get_str("user.objectId", req.data).unwrap_or("");
            if userId != req.auth.user.as_ref().unwrap().id {
                return Err(Error::Internal("".to_string()));
                // throw new Parse.Error(Parse.Error.INVALID_KEY_NAME);
            }
        } else if req.data.contains_key("installationId") {
            return Err(Error::Internal("".to_string()));
        // throw new Parse.Error(Parse.Error.INVALID_KEY_NAME);
        } else if req.data.contains_key("sessionToken") {
            return Err(Error::Internal("".to_string()));
            // throw new Parse.Error(Parse.Error.INVALID_KEY_NAME);
        }
    }
    //   if (!this.query && !this.auth.isMaster) {
    //     const additionalSessionData = {};
    //     for (var key in this.data) {
    //       if (key === 'objectId' || key === 'user') {
    //         continue;
    //       }
    //       additionalSessionData[key] = this.data[key];
    //     }
    //     const {
    //       sessionData,
    //       createSession
    //     } = Auth.createSession(this.config, {
    //       userId: this.auth.user.id,
    //       createdWith: {
    //         action: 'create'
    //       },
    //       additionalSessionData
    //     });
    //     return createSession().then(results => {
    //       if (!results.response) {
    //         throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, 'Error creating session.');
    //       }
    //       sessionData['objectId'] = results.response['objectId'];
    //       this.response = {
    //         status: 201,
    //         location: results.location,
    //         response: sessionData
    //       };
    //     });
    //   }
    Ok(())
}

async fn validate_auth_data<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    if req.class_name != "_User" {
        return Ok(());
    }

    if req.query.is_none() && !req.data.contains_key("authData") {
        let username = req.data.get_str("username").unwrap_or("");
        let password = req.data.get_str("password").unwrap_or("");

        if username == "" {
            return Err(Error::BadFormat(String::new()));
            // throw new Parse.Error(Parse.Error.USERNAME_MISSING, 'bad or missing username');
        }
        if password == "" {
            return Err(Error::BadFormat(String::new()));
            // throw new Parse.Error(Parse.Error.PASSWORD_MISSING, 'password is required');
        }
    }
    //   if (this.data.authData && !Object.keys(this.data.authData).length || !Object.prototype.hasOwnProperty.call(this.data, 'authData')) {
    //     // Handle saving authData to {} or if authData doesn't exist
    //     return;
    //   } else if (Object.prototype.hasOwnProperty.call(this.data, 'authData') && !this.data.authData) {
    //     // Handle saving authData to null
    //     throw new Parse.Error(Parse.Error.UNSUPPORTED_SERVICE, 'This authentication method is unsupported.');
    //   }

    // let auth_data = req.data.get_document("authData").unwrap_or(doc! {});
    // let providers = auth_data.keys();

    // if providers.len() > 0 {
    //     // const canHandleAuthData = providers.reduce((canHandle, provider) => {
    //     //   var providerAuthData = authData[provider];
    //     //   var hasToken = providerAuthData && providerAuthData.id;
    //     //   return canHandle && (hasToken || providerAuthData == null);
    //     // }, true);
    //     // if (canHandleAuthData) {
    //     //   return this.handleAuthData(authData);
    //     // }
    // }
    Ok(())
    //   throw new Parse.Error(Parse.Error.UNSUPPORTED_SERVICE, 'This authentication method is unsupported.');
}

async fn run_before_save_trigger<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    Ok(())
}

async fn delete_email_reset_token_if_needed() {}

async fn validate_schema() {}

async fn set_required_fields_if_needed() {}

async fn transform_user() {}

async fn expand_files_for_existing_objects() {}

async fn destroy_uplicated_sessions<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    if req.class_name != "_Session" || req.query.is_some() {
        return Ok(());
    }

    let user = req.data.get_document("user");
    let installation_id = req.data.get_str("installationId").unwrap_or("");
    let session_token = req.data.get_str("sessionToken").unwrap_or("");

    if user.is_err() || installation_id == "" {
        return Ok(());
    }
    // if !user.object_id {
    //     return Ok(())
    // }
    Ok(())
    // TODO: handle
    // db.destroy_session("").await?;
}

async fn run_database_operation() {}

async fn create_session_token_if_needed<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    if req.class_name != "_User" {
        return Ok(());
      } // Don't generate session for updating user (this.query is set) unless authData exists
    
    
      if req.query.is_some() && !req.data.contains_key("authData") {
        return Ok(());
      } // Don't generate new sessionToken if linking via sessionToken
    
    
      if req.auth.user.is_some() && req.data.contains_key("authData") {
        return Ok(());
      }
    
    //   if (!this.storage['authProvider'] && // signup call, with
    //   this.config.preventLoginWithUnverifiedEmail && // no login without verification
    //   this.config.verifyUserEmails) {
    //     // verification is on
    //     return; // do not create the session token in that case!
    //   }
    Ok(())
    // TODO:
    //   return this.createSessionToken();
}

async fn handle_followup() {}

async fn run_after_save_trigger() {}

// async fn clean_user_auth_data(req: Request<'_>, doc: &Document) -> Document {

// }

pub async fn write<'a>(req: &'a WriteRequest<'a>) -> Result<(), Error> {
    // let acl = util::get_acl(request).await?;
    // util::validate_class_creation(request).await?;
    handle_installation().await?;
    handle_session(req).await?;
    validate_auth_data(req).await?;
    run_before_save_trigger(req).await?;
    // delete_email_reset_token_if_needed().await?;
    // validate_schema().await?;
    // set_required_fields_if_needed().await?;
    // transform_user().await?;
    // expand_files_for_existing_objects().await?;
    destroy_uplicated_sessions(req).await?;
    // run_database_operation().await?;
    create_session_token_if_needed(req).await?;
    // handle_followup().await?;
    // run_after_save_trigger().await?;
    // let response = clean_user_auth_data(doc!{}).await?;
    Ok(())
}
